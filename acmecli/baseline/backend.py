# acmecli/baseline/backend.py

import logging
from flask import Flask
from flask_cors import CORS
import acmecli.baseline.endpoints_delete as delete_module
import acmecli.baseline.download as download_module
import acmecli.baseline.upload as upload_module
import acmecli.baseline.reset as reset_module
import acmecli.baseline.cost as cost_module
import acmecli.baseline.rate as rate_module
import acmecli.baseline.endpoints_search as search_module
import acmecli.baseline.tracks as tracks_module
import acmecli.baseline.endpoints_list as list_module
import acmecli.baseline.endpoints_ingest as ingest_module


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Create a new Flask app
app = Flask(__name__)

# Enable CORS for all routes with permissive settings for development
CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"], "allow_headers": ["Content-Type", "X-Authorization"]}})

# GET /health
@app.route("/health", methods=["GET"])
def health():
    """
    Heartbeat check (BASELINE)
    Lightweight liveness probe. Returns HTTP 200 when the registry API is reachable.
    """
    return "", 200

        
# Register all routes from endpoints_delete.py
for rule in delete_module.app.url_map.iter_rules():
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"delete_{rule.endpoint}",  # Prefix avoids conflicts
            view_func=delete_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from download.py
for rule in download_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"download_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=download_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from upload.py
for rule in upload_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"upload_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=upload_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

#Register all routes from reset.py
for rule in reset_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"reset_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=reset_module.app.view_functions[rule.endpoint],
           methods=rule.methods
       )

# Register all routes from cost.py
for rule in cost_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"cost_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=cost_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from tracks.py
for rule in tracks_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"tracks_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=tracks_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register /rate routes
for rule in rate_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"rate_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=rate_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )


# Register all routes from endpoints_list.py
for rule in list_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"list_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=list_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from search.py
for rule in search_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"search_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=search_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from ingest.py
for rule in ingest_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=f"ingest_{rule.endpoint}",  # Prefix to avoid conflicts
            view_func=ingest_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )




if __name__ == "__main__":
    # Run the combined backend on port 5001
    logging.info("Starting Flask backend server on port 5001")
    app.run(host="0.0.0.0", port=5001, debug=True)
