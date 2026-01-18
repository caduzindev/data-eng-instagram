from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, BackgroundTasks

from .actions.instagram_actions import batch_instagram_accounts


app = FastAPI()

@app.post("/api/batch-instagram-accounts")
async def root(file: UploadFile, background_tasks: BackgroundTasks):
    return await batch_instagram_accounts(file, background_tasks)