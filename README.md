# Automatic ranking for the Cyclingrace racing series

## How do I determine a participant's cluster for the TT event

To identify a participant's cluster in the TT race, I use the following information, in order of priority:

1. A participant's cluster in a group race held on the same weekend as the TT
2. The cluster from the previous race
3. A cluster from the official list that was relevant at the time of the race

## How to use

```bash
python scripts/calculate_rating.py race-results-config.yaml
```
