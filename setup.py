from setuptools import setup, find_packages

# File di setup necessario per installare il progetto in editable mode, in questo modo è possibile consentire 
# all'interprete Python di considerare src/avm/ come package ed evitare di dover importare nel Python Path (sys.path)
# src/ ogni volta per fare in modo che gli import siano correttamente risolti

setup(
    name="automatic-vehicle-monitoring",
    version="0.1.0",
    description="Simulatore AVM per flotta autobus",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.11",
)
