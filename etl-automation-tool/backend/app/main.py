from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os

from app.etl.extractor import extract_csv
from app.etl.cleaner import clean_data
from app.etl.transformer import transform_data
from app.etl.reporter import generate_report
app = FastAPI(title="Simple ETL Tool")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.post("/run-etl")
async def run_etl(file: UploadFile = File(...)):

    try:
        df = extract_csv(file.file)
        cleaned_df, clean_report = clean_data(df)
        final_df = transform_data(cleaned_df)
        os.makedirs("data/processed", exist_ok=True)
        output_path = "data/processed/cleaned_data.csv"
        final_df.to_csv(output_path, index=False)
        return generate_report(clean_report)
    except Exception as error:
        return {
            "status": "failed",
            "message": str(error)
        }
@app.get("/download-cleaned")
def download_cleaned():

    file_path = "data/processed/cleaned_data.csv"

    if not os.path.exists(file_path):
        return {"error": "Please run ETL first."}

    return FileResponse(
        path=file_path,
        filename="cleaned_data.csv",
        media_type="text/csv"
    )


