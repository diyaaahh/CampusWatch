import os
import json 
import logging
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta


logging.basicConfig(level=logging.INFO)

BATCH_SIZE = int (os.getenv("BATCH_SIZE", "500"))


DB_DSN = os.getenv("DATABASE_URL") #postgres://postgres:diya@localhost:5433/CampusWatch

# getting all the pending requests from database 
def get_pending_requests(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT * FROM user_deletion_requests
            WHERE verified = true AND status ='PENDING'
            ORDER_BY requested_at
         """)
        return cur.fetchall()


#setting the status of requested user to 'IN_PROGRESS'    
def mark_request_in_progress(conn, req_id):
    with conn.cursor() as cur:
        cur.execute(""" 
            UPDATE user_deletion_requests
                    SET status = 'IN_PROGRESS'
                    WHERE id=%s AND status ="PENDING" """, (req_id))
        return cur.fetchone() is not None
    
#soft deleting user 
def soft_delete_user(conn, user_id):
    ts= datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET deleted_at=%s WHERE id=%s AND deleted_at IS NULL",(ts,user_id))
        conn.commit()

#hard deleting user info(deleting user info from other tables as well
def hard_delete_user(conn, table_name, user_col, user_id, batch_size):
    with conn.cursor() as cur:
        cur.execute(f""" 
            DELETE FROM {table_name}
            WHERE {user_col}= %s AND deleted_at IS NOT NULL
            LIMIT %s """,(user_id,batch_size))
        rs=cur.fetchall()
        conn.commit()
        return len(rs)
    
#inserting the deletion info in the audit table 
def insert_audit_log(conn, request_id, rows_deleted, status):
    with conn.cursor() as cur:
        cur.execute(""" 
            INSERT INTO deletion_audit (request_id, rows_deleted, status,started_at,completed_at)
            VALUES(%s, %s, %s, %s, now(), now()))""", (request_id, json.dumps(rows_deleted), status))
        conn.commit()
    
#sequence of operations
def process_request(conn, request):
    req_id = request['id']
    user_id = request['user_id']
    logging.info(f"Processing deletion request {req_id} for user {user_id}")

    if not mark_request_in_progress(conn, req_id):
        logging.info("Could mark the request as IN _PROGRESS")
        return
    
    try:
        soft_delete_user(conn, user_id)

        if not request.get('hard_delete_at'):
            with conn.cursor() as cur:
                cur.execute(""" UPDATE user_deletion_requests SET scheduled_hard_delete_at=now() + interval '48 hours' WHERE id=%s """,(req_id))
                conn.commit()
        insert_audit_log(conn, req_id, {'soft_deleted': 1}, "SUCCESS", "Soft deletion completed")
    
    except Exception as e:
        logging.exception("Error occured during soft deltion")
        with conn.cursor() as cur:
            cur.execute(" UPDATE user_deletion_requests SET status='FAILED' WHERE id=%s", (req_id))
            conn.commit()
        insert_audit_log(conn, req_id, {}, "FAILED", str(e))

def run_hard_delete(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(""" 
        SELECT * FROM user_deletion_requests
            WHERE status='IN_PROGRESS' AND hard_delete_at <= now()
                    LIMIT 10""")
        rows= cur.fetchall()
    for req in rows:
        req_id = req['id']
        user_id = req['user_id']
        logging.info(f"Processing hard deletion for request {req_id} and user {user_id}")
        total = {}
        try:
            tables_to_clean = [('users', 'user_id'),('devices', 'owner_id')]
            for table,col in tables_to_clean:
                deleted=0
                while True:
                    n=hard_delete_user(conn, table, col, user_id, BATCH_SIZE)
                    deleted +=n
                    if n< BATCH_SIZE:
                        break
                    time.sleep(1)
                total[table]=deleted

                with conn.cursor() as cur:
                    cur.execute(""" UPDATE user_deletion_requests SET status='COMPLETED' completed_at=now() WHERE id=%s """,(req_id))
                    conn.commit()

                    insert_audit_log(conn, req_id, total , "SUCCESS", "Hard delete completed")
                    logging.info(f"Hard deletioon completed for user with userid {user_id}")
                
        except Exception as e:
            logging.exception("Hard deletion failed for {req_id}")
            with conn.cursor() as cur:
                cur.execute(" UPDATE user_deletion_requests SET status='FAILED' WHERE id=%s", (req_id))
                conn.commit()
            insert_audit_log(conn , req_id, total, "FAILED", str(e))

def main():
    conn = psycopg2.connect(DB_DSN)
    try:
        pending = get_pending_requests(conn)
        for req in pending:
            process_request(conn, req)

            run_hard_delete(conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()





