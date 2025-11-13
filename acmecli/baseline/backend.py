# acmecli/baseline/backend.py

from flask import Flask
import acmecli.baseline.download as download_module
import acmecli.baseline.upload as upload_module

# Create a new Flask app
app = Flask(__name__)

# Register all routes from download.py
for rule in download_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=rule.endpoint,
            view_func=download_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

# Register all routes from upload.py
for rule in upload_module.app.url_map.iter_rules():
    # Skip the static route
    if rule.endpoint != 'static':
        app.add_url_rule(
            rule.rule,
            endpoint=rule.endpoint,
            view_func=upload_module.app.view_functions[rule.endpoint],
            methods=rule.methods
        )

if __name__ == "__main__":
    # Run the combined backend on port 5001
    app.run(host="0.0.0.0", port=5001, debug=True)