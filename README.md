# focus-demo-sim
This repo is for simulating the sim trains on the demo dashboard. It replays train telemetry from the cloud for trains defined in `train_config.json` file 

## Installation and Usage
```
git clone https://github.com/Humatics/focus-demo-sim.git
cd focus-demo-sim
pip install -r requirements.txt
python sim_client.py
```

### Optional arguments 
```
-s    ->   Start time of query: eg - 2023-12-01T12:01:02.000Z
-d    ->   Duration after start time to replay data: eg- 1:30:00
-l    ->   If flag is set, replay will loop after completion
```

Optional call specifying start and duration: `python sim_client.py -s 2024-07-22T20:00:00.000000Z -d 00:30:00 -l`

#### For Windows
```
python -m pip insall -r requirements.txt
python -m sim_client.py
```




