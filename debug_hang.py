import pandas as pd
from dataintegrity.core.dataset import Dataset
from dataintegrity.core.config import IntegrityConfig
from dataintegrity.integrity.engine import IntegrityEngine
from dataintegrity.policies import POLICY_REGISTRY

df = pd.DataFrame({"a": [1, 2, 3]})
dataset = Dataset(df, source="test")
engine = IntegrityEngine()
result = engine.run(dataset)

print("Engine run completed.")
d1 = result.to_dict()
print("to_dict 1 completed.")

pol = POLICY_REGISTRY["production"]
res = pol.evaluate(d1)
print("Evaluate 1 completed.")

result.policy_evaluations.append(res)
d2 = result.to_dict()
print("to_dict 2 completed.")
