from flask import Flask, jsonify
import datetime as dt
import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

# Create an engine to connect to your database
engine = create_engine("sqlite:///hawaii.sqlite")

# Reflect the database tables
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create a Flask app
app = Flask(__name__)

# Define routes
@app.route("/")
def home():
    """List all available routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the JSON representation of precipitation data for the last 12 months."""
    session = Session(engine)
    
    # Calculate the date 1 year ago from the last data point in the database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    one_year_ago = dt.datetime.strptime(last_date, "%Y-%m-%d") - dt.timedelta(days=365)
    
    # Query precipitation data for the last 12 months
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all()
    session.close()
    
    # Convert the results to a dictionary
    prcp_data = {date: prcp for date, prcp in results}
    
    return jsonify(prcp_data)

@app.route("/api/v1.0/stations")
def stations():
    """Return a JSON list of station names."""
    session = Session(engine)
    
    # Query all station names
    results = session.query(Station.station).all()
    session.close()
    
    # Convert the results to a list
    station_list = list(np.ravel(results))
    
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a JSON list of temperature observations (tobs) for the previous year."""
    session = Session(engine)
    
    # Query the most active station
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).\
                          order_by(func.count(Measurement.station).desc()).first()[0]
    
    # Calculate the date 1 year ago from the last data point for the most active station
    last_date = session.query(Measurement.date).filter(Measurement.station == most_active_station).\
                order_by(Measurement.date.desc()).first()[0]
    one_year_ago = dt.datetime.strptime(last_date, "%Y-%m-%d") - dt.timedelta(days=365)
    
    # Query temperature observations for the previous year for the most active station
    results = session.query(Measurement.date, Measurement.tobs).\
              filter(Measurement.station == most_active_station, Measurement.date >= one_year_ago).all()
    session.close()
    
    # Convert the results to a list of dictionaries
    tobs_data = [{"date": date, "tobs": tobs} for date, tobs in results]
    
    return jsonify(tobs_data)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temperature_range(start, end=None):
    """Return a JSON list of minimum, average, and maximum temperatures for a given date range."""
    session = Session(engine)
    
    # Define the base query for calculating temperature statistics
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    
    if end:
        # Query temperature statistics for a specified date range
        results = session.query(*sel).filter(Measurement.date >= start, Measurement.date <= end).all()
    else:
        # Query temperature statistics for dates greater than or equal to the start date
        results = session.query(*sel).filter(Measurement.date >= start).all()
    
    session.close()
    
    # Convert the results to a dictionary
    temp_stats = {
        "start_date": start,
        "end_date": end if end else None,
        "TMIN": results[0][0],
        "TAVG": results[0][1],
        "TMAX": results[0][2]
    }
    
    return jsonify(temp_stats)

if __name__ == "__main__":
    app.run(debug=True)
