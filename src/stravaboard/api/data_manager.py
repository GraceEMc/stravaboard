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
        per_page = min(n, 200)  # Strava max per_page is typically 200
        page = 1
        activities_list = []
        # activities = requests.get(
        #     self.ACTIVITIES_URL, headers=header, params=params
        # ).json()
        # activities = pd.json_normalize(activities)

        # self.data = activities
        activities_list = []
        try:
            while len(activities_list) < n:
                params = {"per_page": per_page, "page": page}
                resp = requests.get(self.ACTIVITIES_URL, headers=header, params=params)
                if resp.status_code != 200:
                    # store response for debugging and stop
                    try:
                        err = resp.json()
                    except Exception:
                        err = resp.text
                    print(f"⚠️ Strava API error: status={resp.status_code} body={err}")
                    break

                payload = resp.json()
                # If Strava returns an error object instead of a list
                if isinstance(payload, dict) and ("message" in payload or "errors" in payload):
                    print(f"⚠️ Strava API returned error object: {payload}")
                    break

                if not payload:
                    # no more activities
                    break

                activities_list.extend(payload)
                # stop if fewer results than per_page (end of pages)
                if len(payload) < per_page:
                    break
                page += 1

            # Normalize results into a DataFrame (empty list -> empty DataFrame)
            activities = pd.json_normalize(activities_list) if activities_list else pd.DataFrame()
            self.data = activities

        except requests.RequestException as e:
            print(f"⚠️ Network error while fetching activities: {e}")
            self.data = pd.DataFrame()


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

