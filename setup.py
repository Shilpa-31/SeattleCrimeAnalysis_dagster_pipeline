from setuptools import setup, find_packages

setup(
    name="SeattleCrimeData_analysis",
    packages=find_packages(exclude=["SeattleCrimeData_analysis_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-webserver",
        "pandas",
        "geopandas",
        "shapely",
        "sqlalchemy",
        "pymongo",
        "streamlit",
        "folium",
        "psycopg2-binary"
    ],
     extras_require={"dev": ["dagster-webserver", "pytest"]},
)

# python3 -m venv .venv
# source .venv/bin/activate
# dagster dev

# streamlit run dashboard/app.py
# nohup streamlit run dashboard/app.py &
# tail -f nohup.out ---> to check logs

# ps aux | grep streamlit -->Stop the background Streamlit process
# kill 12345