import subprocess
import sys
#description: use builty to import the data, compile to merge the files into one large file, elevation to filter builty for flood/elevation only, merge_loop to merge with attom data, and parquetdta to convert to dta for stata analysis
scripts = [
    "builty.py",
    "compile.py", 
    "elevation.py",
    "merge_loop.py",
    "parquetdta.py"
]

for script in scripts:
    print(f"\n{'='*40}")
    print(f"Running {script}...")
    print('='*40)
    
    result = subprocess.run([sys.executable, script], check=True)
    print(f"Done: {script}")

print("\nAll scripts completed.")