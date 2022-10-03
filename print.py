import os
from pathlib import Path
import numpy as np
import pandas as pd

data_file = Path('employees.csv')
df = pd.read_csv(data_file)