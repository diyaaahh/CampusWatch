from fastapi import FastAPI, HTTPException,Request, status
from datetime import date 

app=FastAPI()

#Set of valid API keys 
valid_api_keys={
    "first_key":{"active":True, "owner":"camera1", "expires_at":"2025-09-30"},
    "second_key":{"active":True, "owner":"camera2", "expires_at":"2025-09-10"},
}

def validate_api_key(key: str):
    #checking if the key is valid
    if key not in valid_api_keys:
        return False , "Invalid API key"
    
    #checking if the key is active 
    key_info = valid_api_keys[key]
    if not key_info["active"]:
        return False , "API key is inactive"
    
    #checking if the key has expired
    if date.today() > date.fromisoformat(key_info["expires_at"]):
        return False, "API key has expired"
    
    return True , "API key is valid"

@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    #getting the API key from request header
    api_key = request.headers.get("the_api_key")
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED , detail="API key is missing")
    
    is_valid, message = validate_api_key(api_key)
    if not is_valid :
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=message)
    
    return await call_next(request)

@app.get("/camera/register")
async def register_camera():
    return {"message":"Camera registered succesfully!"}
    

# Running the service :
# python -m venv venv
# source venv/bin/activate 
# pip install fastapi uvicorn 
# uvicorn apiAuthentication:app --reload 
# curl -H "the_api_key: first_key" http://127.0.0.1:8000/camera/register
# curl -H "the_api_key: second_key" http://127.0.0.1:8000/camera/register