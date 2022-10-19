# mysetup.py
from distutils.core import setup
import py2exe

sys.path.append("D:\\a-sk\\programCode\\test\\foo.csv")
setup(window=["test.py"])
