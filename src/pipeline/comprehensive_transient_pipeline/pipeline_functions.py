import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os
import subprocess
import time
import warnings
import yaml
from typing import Any

from matplotlib.lines import Line2D

EXTERNAL_PYTHON = "/home/gamma/envs/cosipy_laura/bin/python"
def execute_bindata_grb(cosipy_yaml_input,pipeline_input_file):
    import cosipy
    from cosipy.pipeline.task.task import cosi_bindata
    import subprocess
    import sys
    from yayc import Configurator
    
    print('exampe BINNING ')
    print('##################',cosipy_yaml_input)
    print('##################',pipeline_input_file)
    print('##################',txt_config)
    
    full_config = Configurator.open(cosipy_yaml_input)
    t_scan_start_source=full_config["general_pipeline_config"]["t_scan_start_source"]
    t_scan_stop_source=full_config["general_pipeline_config"]["t_scan_stop_source"]
    directory_output =full_config["general_pipeline_config"]["directory_output"]
     
    args=['--config',cosipy_yaml_input,'--config_group','bindata_soubk','--overwrite', '--suffix','galbk_grbdc3','--output-dir',directory_output,'--tmin', str(t_scan_start_source), '--tmax', str(t_scan_stop_source)]
    cosi_bindata (argv=args)
    
