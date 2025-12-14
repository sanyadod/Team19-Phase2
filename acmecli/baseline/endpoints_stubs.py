"""from flask import Flask, abort

app = Flask(__name__)

@app.route("/artifact/<artifact_type>/<artifact_id>/lineage", methods=["GET"])
def lineage_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")

@app.route("/artifact/<artifact_type>/<artifact_id>/cost", methods=["GET"])
def cost_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")

@app.route("/artifact/<artifact_type>/<artifact_id>/rate", methods=["GET"])
def rate_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")

@app.route("/artifact/<artifact_type>/<artifact_id>/access", methods=["GET"])
def access_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")

@app.route("/artifact/<artifact_type>/<artifact_id>/permissions", methods=["GET"])
def permissions_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")

@app.route("/artifact/<artifact_type>/<artifact_id>/license-check", methods=["POST"])
def license_check_stub(artifact_type, artifact_id):
    abort(501, description="Not implemented")
"""
