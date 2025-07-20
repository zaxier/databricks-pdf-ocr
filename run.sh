python -m volsync create-volume --catalog zaxier_dev --schema pdf_ocr --volume pdf_documents
python -m volsync create-volume --catalog zaxier_dev --schema pdf_ocr --volume checkpoints
python -m volsync upload --catalog zaxier_dev --schema pdf_ocr --volume pdf_documents --pattern "*.pdf" data


