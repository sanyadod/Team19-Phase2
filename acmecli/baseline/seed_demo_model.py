# scripts/seed_demo_model.py

from acmecli.baseline.modeldb import put_model_from_phase1

if __name__ == "__main__":
    demo = {
        "id": "48472749248", 
        "version": "1.0.0",

        # Phase 1 scores
        "size_score": 0.8,
        "license_score": 0.9,
        "rampup_score": 0.7,
        "bus_factor": 0.6,
        "dataset_and_code": 0.8,
        "dataset_quality": 0.85,
        "code_quality": 0.9,
        "perf_claims": 0.75,

        # Optional Phase 2 extras
        "reproducibility": 0.5,
        "reviewedness": -0.1,
        "parents": [],
    }

    put_model_from_phase1(demo)
    print("Seeded model", demo["id"])
