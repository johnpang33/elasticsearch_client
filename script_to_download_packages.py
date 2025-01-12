import pkg_resources 

# Get all installed packages 

installed_packages = [pkg.key for pkg in pkg_resources.working_set] 

# Save only package names to requirements.txt 
with open("requirement.txt", "w") as file:
	file.write("\n".join(installed_packages))