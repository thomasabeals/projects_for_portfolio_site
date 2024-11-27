import requests
import pandas as pd
import json
from typing import Dict, Any, List, Union
from requests.auth import AuthBase
import logging

class APIClient:
    def __init__(self, base_url: str, auth: AuthBase = None):
        self.base_url = base_url
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
        self.logger = logging.getLogger(__name__)

    def get(self, endpoint: str, params: Dict[str, Any] = None) -> requests.Response:
        """Make a GET request to the API."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params)
        self.logger.info(f"GET request to {url}")
        response.raise_for_status()
        return response

    def post(self, endpoint: str, data: Dict[str, Any]) -> requests.Response:
        """Make a POST request to the API."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.post(url, json=data)
        self.logger.info(f"POST request to {url}")
        response.raise_for_status()
        return response

    def put(self, endpoint: str, data: Dict[str, Any]) -> requests.Response:
        """Make a PUT request to the API."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.put(url, json=data)
        self.logger.info(f"PUT request to {url}")
        response.raise_for_status()
        return response

    def delete(self, endpoint: str) -> requests.Response:
        """Make a DELETE request to the API."""
        url = f"{self.base_url}/{endpoint}"
        response = self.session.delete(url)
        self.logger.info(f"DELETE request to {url}")
        response.raise_for_status()
        return response

class DataProcessor:
    @staticmethod
    def to_dataframe(data: Union[List[Dict], Dict]) -> pd.DataFrame:
        """Convert API response to a pandas DataFrame."""
        return pd.DataFrame(data)

    @staticmethod
    def clean_data(df: pd.DataFrame) -> pd.DataFrame:
        """Perform basic data cleaning operations."""
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values (you might want to customize this based on your needs)
        df = df.fillna(method='ffill').fillna(method='bfill')
        
        # Convert date columns to datetime
        date_columns = df.select_dtypes(include=['object']).columns
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
            except ValueError:
                pass  # Column couldn't be converted to datetime
        
        return df

    @staticmethod
    def summarize_data(df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for the dataset."""
        summary = {
            "shape": df.shape,
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.to_dict(),
            "missing_values": df.isnull().sum().to_dict(),
            "numeric_summary": df.describe().to_dict(),
        }
        return summary

def main():
    # Example usage
    api_client = APIClient("https://api.example.com", auth=("username", "password"))
    
    # Fetch data from an API
    response = api_client.get("data_endpoint")
    
    # Process the data
    processor = DataProcessor()
    df = processor.to_dataframe(response.json())
    df_clean = processor.clean_data(df)
    
    # Generate summary
    summary = processor.summarize_data(df_clean)
    
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()

