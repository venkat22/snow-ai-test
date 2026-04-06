import subprocess, os
pw = "ChangeMe" + chr(33) + "Str0ng" + chr(35) + "2026"
print(f"Password: {pw}")
print(f"Length: {len(pw)}")
with open("/tmp/sf_pass.txt", "w") as f:
    f.write(pw)

# Update the secret using Azure CLI
az = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
result = subprocess.run(
    [az, "containerapp", "secret", "set",
     "--name", "snow-marketplace",
     "--resource-group", "rg-snow-marketplace",
     "--secrets", f"sf-password={pw}"],
    capture_output=True, text=True
)
print("STDOUT:", result.stdout[-200:] if result.stdout else "")
print("STDERR:", result.stderr[-200:] if result.stderr else "")
print("RC:", result.returncode)
