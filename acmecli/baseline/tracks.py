# acmecli/baseline/tracks.py

from flask import Flask, jsonify
import logging

app = Flask(__name__)
logger = logging.getLogger(__name__)

# Define the tracks that this implementation supports
# Empty list - no tracks planned
PLANNED_TRACKS = []


@app.get("/tracks")
def get_tracks():
    """
    GET /tracks
    Get the list of tracks a student has planned to implement in their code.
    
    Returns:
        {
            "plannedTracks": []
        }
    """
    try:
        response = {
            "plannedTracks": PLANNED_TRACKS
        }
        return jsonify(response), 200
    except Exception as e:
        logger.error(f"Error retrieving tracks: {e}", exc_info=True)
        return jsonify({"error": "The system encountered an error while retrieving the student's track information."}), 500


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5003, debug=True)

