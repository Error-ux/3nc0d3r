import json
import os

def main():
    # Read settings from environment variables
    config_data = {
        "gh_token": os.environ.get("GITHUB_TOKEN", os.environ.get("GH_TOKEN", "")).strip(),
        "gh_owner": os.environ.get("GITHUB_REPOSITORY_OWNER", os.environ.get("GH_OWNER", "")).strip(),
        "gh_repo": os.environ.get("GITHUB_REPOSITORY", "").split("/")[-1] if os.environ.get("GITHUB_REPOSITORY") else "3nc0d3r",
        "redis_url": os.environ.get("REDIS_URL", "").strip(),
        "redis_token": os.environ.get("REDIS_TOKEN", "").strip()
    }
    
    # Try fallback to git config or config.py if empty
    if not config_data["gh_owner"]:
        config_data["gh_owner"] = os.environ.get("USER", "")
        
    # Write to local config.json
    output_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(output_path, "w") as f:
        json.dump(config_data, f, indent=4)
    print(f"Generated config.json at {output_path} with keys: {list(config_data.keys())}")

if __name__ == "__main__":
    main()
