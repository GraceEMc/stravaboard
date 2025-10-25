from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
import requests


class DataManager(ABC):
    """
    Interface for classes that retrieve data from Strava.
    """

    @abstractmethod
    def get_data(self, access_token: str, n: int) -> None:
        """
        Retrieve data from Strava.
        """
        pass

    @abstractmethod
    def tidy_data(self) -> None:
        """
        Tidy the retrieved Strava data.
        """
        pass


class ActivitiesManager(DataManager):
    """
    Responsible for requesting and storing activities data.
    """

    ACTIVITIES_URL = "https://www.strava.com/api/v3/athlete/activities"

    def get_data(self, access_token: str, n: int = 200) -> None:
        """
        Download Strava activity data.

        Queries the Strava API then stores the obtained activity data as a DataFrame.

        Args:
            access_token: Strava access token
            n: Maximum number of activities to retrieve, by default 200.
        """
        header = {"Authorization": "Bearer " + access_token}
        params = {"per_page": n, "page": 1}
        activities = requests.get(
            self.ACTIVITIES_URL, headers=header, params=params
        ).json()
        activities = pd.json_normalize(activities)

        self.data = activities

    def tidy_data(self) -> None:
        """
        Tidy the activity data.
    
        Convert speed, distance, time and date columns to human-interpretable units.
        Safely handles missing fields from the Strava API.
        """
    
        activities = self.data
    
        # Ensure it's a DataFrame and not empty
        if not isinstance(activities, pd.DataFrame) or activities.empty:
            print("⚠️ No activity data to tidy.")
            self.data = pd.DataFrame()
            return
    
        # Create derived columns only if source data exists
        if "elapsed_time" in activities.columns:
            activities["elapsed_min"] = (activities["elapsed_time"] / 60).round(2)
        else:
            print("⚠️ Missing 'elapsed_time' column.")
            activities["elapsed_min"] = None
        
    
        if "distance" in activities.columns:
            activities["distance_km"] = (activities["distance"] / 1000).round(2)
        else:
            print("⚠️ Missing 'distance' column.")
            activities["distance_km"] = None
    

        if {"elapsed_min", "distance_km"}.issubset(activities.columns):
            activities["speed_mins_per_km"] = (
                activities["elapsed_min"] / activities["distance_km"]
            ).replace([np.inf, -np.inf], np.nan).round(2)
        else:
            activities["speed_mins_per_km"] = np.nan
    
        # Parse and format dates
        if "start_date_local" in activities.columns:
            activities["date"] = (
                activities["start_date_local"]
                .astype(str)
                .str.replace("T.*", "", regex=True)
            )
            activities["date"] = pd.to_datetime(
                activities["date"], errors="coerce", infer_datetime_format=True
            )
        else:
            print("⚠️ Missing 'start_date_local' column.")
            activities["date"] = None
    
        self.data = activities

