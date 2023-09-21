# with a deployment, if not writing out to absolute file path
# or saving to Cloud or Docker volume,
# output will be written to temporary directory and deleted
# so we'll just persist the result to storage

import httpx
from prefect import flow, task


@task
def fetch_cat_fact():
    return httpx.get("https://catfact.ninja/fact?max_length=140").json()["fact"]


@task(persist_result=True)
def formatting(fact: str):
    return fact.title()

@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    print(f"Most recent temp C: {most_recent_temp} degrees")
    return most_recent_temp

@flow
def pipe(lat: float, lon: float):
    fact = fetch_cat_fact()
    formatted_fact = formatting(fact)
    temp = fetch_weather(lat, lon)
    print(formatted_fact)

if __name__ == "__main__":
    pipe(40.7,111.7)
