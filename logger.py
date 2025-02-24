import logging

# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),  # Save logs to a file
        logging.StreamHandler()  # Show logs in the console
    ],
    encoding="utf-8"
)

# Create a logger instance
logger = logging.getLogger(__name__)
