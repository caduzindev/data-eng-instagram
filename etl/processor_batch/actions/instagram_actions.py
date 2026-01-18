from fastapi import UploadFile, HTTPException, BackgroundTasks
from ..services.instagram_service import instagram_service

async def batch_instagram_accounts(file: UploadFile, background_tasks: BackgroundTasks):
    mime_type = file.content_type
    if mime_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Extensão de arquivo invalida")

    contents = file.file.read()

    background_tasks.add_task(instagram_service.csv_batch_accounts_post_comments, contents)

    return {
        "message": "Arquivo recebido. O processamento iniciou em segundo plano.",
        "filename": file.filename
    }