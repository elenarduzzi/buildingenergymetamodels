# ANN – two-target version (Heating & Cooling)
# no vertex data 
# hard codes list of input features used for training. 
# FOR REPORT. WITH UNSCALED ERROR 

import joblib, pandas as pd, torch, numpy as np
from torch import nn, optim
from pathlib import Path
from tqdm import trange
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import wandb
import time  

# hyper-parameters
LR        = 1e-4
HIDDEN    = 32
EPOCHS    = 20_000
LOG_INT   = 10
PATIENCE  = 400
DELTA     = 0.0


# paths 
ROOT_INPUT     = Path(r"C:\thesis\CLEAN_WORKFLOW\4_data_struct_out\6_scale_filtered\2020")

TRAIN_CSV      = ROOT_INPUT / "train_scale.csv"  
VAL_CSV        = ROOT_INPUT / "validate_scale.csv"
TEST_CSV       = ROOT_INPUT / "test_scale.csv"
MM_PARAMS      = ROOT_INPUT / "mm_params.joblib"

WANB_PROJECT  = "ann_REPORT_D"
WANB_RUN       = "D_v4"

ROOT_OUTPUT = Path(r"C:\thesis\CLEAN_WORKFLOW\5_ml_out\ann_D")

MODEL_OUT_DIR  = ROOT_OUTPUT / WANB_RUN
MODEL_OUT_DIR.mkdir(parents=True, exist_ok=True)

TIME_LOG = MODEL_OUT_DIR / "training_time.txt" 

MODEL_PATH = MODEL_OUT_DIR / "ann_model.pth"
PRED_PATH  = MODEL_OUT_DIR / "ann_predictions.csv"
ERROR_OUT  = MODEL_OUT_DIR / "error_metrics.csv"

# id columns and targets, everything else is features
ID_COLS     = ["Pand ID", "Archetype ID", "Construction Year"] #, "Surface Index", "Surface Type"
TARGET_COLS = ["Annual Heating", "Annual Cooling"]

"""
# full lsit to use for training
FEATURE_COLS = ["Number of Floors", "Wall Area", "Roof Area (Flat)", "Roof Area (Sloped)", "Floor Area",
                "Shared Wall Area", "Building Height (70%)", "Building Volume", "Total Floor Area", 
                "Compactness Ratio", 
                "G Insulation", "F Insulation", "R Insulation", 
                "Infiltration", "WWR", "U_Factor", "SHGC", 
                "temp_avg_1", "temp_avg_2", "temp_avg_3", "temp_avg_4", "temp_avg_5", "temp_avg_6", 
                "temp_avg_7", "temp_avg_8", "temp_avg_9", "temp_avg_10", "temp_avg_11", "temp_avg_12", 
                "rad_avg_1", "rad_avg_2", "rad_avg_3", "rad_avg_4", "rad_avg_5", "rad_avg_6", 
                "rad_avg_7", "rad_avg_8", "rad_avg_9", "rad_avg_10", "rad_avg_11", "rad_avg_12", 

                OR ANNUAL WEATHER
                "avg_annual_temp", "avg_annual_rad"
]

"""

# Custom features to use for training
FEATURE_COLS = ["Number of Floors", "Wall Area", "Floor Area",
                "Shared Wall Area", "Building Height (70%)",
                "G Insulation", "F Insulation", "R Insulation", 
                "Infiltration", "WWR", "U_Factor", "SHGC"
]

# mape helper
def mape(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-6) -> float:
    denom = np.clip(np.abs(y_true), eps, None)
    return float(np.mean(np.abs((y_true - y_pred) / denom)))

# load inputs and tensors
X_COLS = FEATURE_COLS.copy()

train_df = pd.read_csv(TRAIN_CSV, dtype={c: "string" for c in ID_COLS})
val_df   = pd.read_csv(VAL_CSV  , dtype={c: "string" for c in ID_COLS})
test_df  = pd.read_csv(TEST_CSV , dtype={c: "string" for c in ID_COLS})

X_train = torch.tensor(train_df[X_COLS].values, dtype=torch.float32)
y_train = torch.tensor(train_df[TARGET_COLS].values , dtype=torch.float32)
X_val   = torch.tensor(val_df  [X_COLS].values, dtype=torch.float32)
y_val   = torch.tensor(val_df  [TARGET_COLS].values , dtype=torch.float32)
X_test  = torch.tensor(test_df [X_COLS].values, dtype=torch.float32)
y_test  = torch.tensor(test_df [TARGET_COLS].values , dtype=torch.float32)

# MODEL 
class ANN(nn.Module):
    def __init__(self, in_dim, hidden, out_dim=2): # IF CHANGE NUMB OF LAYERS UPDATE OUT_DIM FOR LOGGING
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(in_dim, hidden), nn.ReLU(),
            nn.Linear(hidden, hidden), nn.ReLU(),

            nn.Linear(hidden, out_dim)
        )
    def forward(self, x): return self.net(x)

model     = ANN(len(X_COLS), HIDDEN, out_dim=len(TARGET_COLS))
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=LR)

# wandb log
run = wandb.init(
    project= WANB_PROJECT,
    name=WANB_RUN,
    config=dict(lr=LR, hidden=HIDDEN, epochs=EPOCHS,
                log_interval=LOG_INT, patience=PATIENCE,
                delta=DELTA, layers=2)
)

# training loop
start_time = time.time()

best_loss, best_epoch = float("inf"), -1
bar = trange(EPOCHS, desc="training", leave=False)

for epoch in bar:
    model.train(); optimizer.zero_grad()
    loss_train = criterion(model(X_train), y_train)
    loss_train.backward(); optimizer.step()

    model.eval()
    with torch.no_grad():
        pred_val = model(X_val)
        loss_val = criterion(pred_val, y_val)

    bar.set_postfix(train=loss_train.item(), val=loss_val.item())

    wandb.log({
        "epoch": epoch + 1,
        "train_mse": loss_train.item(),
        "val_mse": loss_val.item()
    }, commit=False)

    if (epoch + 1) % LOG_INT == 0:
        y_true = y_val.numpy()
        y_pred = pred_val.numpy()
        wandb.log({
            "val_mae_heating":  mean_absolute_error(y_true[:, 0], y_pred[:, 0]),
            "val_mae_cooling":  mean_absolute_error(y_true[:, 1], y_pred[:, 1]),
            "val_rmse_heating": np.sqrt(mean_squared_error(y_true[:, 0], y_pred[:, 0])),
            "val_rmse_cooling": np.sqrt(mean_squared_error(y_true[:, 1], y_pred[:, 1])),
            "val_r2_heating":   r2_score(y_true[:, 0], y_pred[:, 0]),
            "val_r2_cooling":   r2_score(y_true[:, 1], y_pred[:, 1]),
            "val_mape_heating": mape(y_true[:, 0], y_pred[:, 0]),
            "val_mape_cooling": mape(y_true[:, 1], y_pred[:, 1])
        })

    # early stopping on validation 
    if loss_val.item() < best_loss - DELTA:
        best_loss, best_epoch = loss_val.item(), epoch
        torch.save(model.state_dict(), MODEL_PATH)
    elif (epoch - best_epoch) >= PATIENCE:
        break


elapsed = time.time() - start_time                
with open(TIME_LOG, "w", encoding="utf-8") as f:  
    f.write(f"Training stopped at epoch {epoch+1}\n")
    f.write(f"Elapsed time: {elapsed/60:.2f} minutes "
            f"({elapsed:.2f} s)\n")
print("Training-time log saved to:", TIME_LOG)



# inverse scale predictions
model.load_state_dict(torch.load(MODEL_PATH))
model.eval()

# final test predictions
with torch.no_grad():
    preds_scaled_test = model(X_test).clamp_min(0).numpy()

mm = joblib.load(MM_PARAMS)
truth_test = y_test.numpy().copy()
preds_test = preds_scaled_test.copy()
for i, col in enumerate(TARGET_COLS):
    vmin, vrange = mm[col]
    truth_test[:, i] = truth_test[:, i] * vrange + vmin
    preds_test[:, i] = preds_test[:, i] * vrange + vmin

truth_heat, truth_cool = truth_test[:, 0], truth_test[:, 1]
pred_heat , pred_cool  = preds_test[:, 0], preds_test[:, 1]

# wandb scatter plots on test set
pand_ids = test_df["Pand ID"].tolist()
arch_ids = test_df["Archetype ID"].tolist()
for label, idx in {"Heating":0, "Cooling":1}.items():
    table = wandb.Table(
        data=[[pid, aid, t, p]
              for pid, aid, t, p in zip(pand_ids, arch_ids,
                                        truth_test[:,idx],
                                        preds_test[:,idx])],
        columns=["Pand ID","Archetype ID",
                 f"True {label}", f"Pred {label}"]
    )
    wandb.log({
        f"scatter_{label.lower()}":
            wandb.plot.scatter(table,
                               x=f"True {label}",
                               y=f"Pred {label}",
                               title=f"{label}: True vs Pred")
    })

# save outputs to wandb 
out_df = pd.concat([
    test_df[ID_COLS].reset_index(drop=True),
    pd.DataFrame(truth_test, columns=[f"{c}_true" for c in TARGET_COLS]),
    pd.DataFrame(preds_test , columns=[f"{c}_pred" for c in TARGET_COLS]),
], axis=1)
out_df.to_csv(PRED_PATH, index=False)

# Re-compute metrics on ORIGINAL scale
mae_heat  = mean_absolute_error(truth_heat , pred_heat)
mae_cool  = mean_absolute_error(truth_cool , pred_cool)
rmse_heat = np.sqrt(mean_squared_error(truth_heat , pred_heat))
rmse_cool = np.sqrt(mean_squared_error(truth_cool , pred_cool))
mape_heat = mape(truth_heat , pred_heat) * 100   # %
mape_cool = mape(truth_cool , pred_cool) * 100   # %
r2_heat   = r2_score(truth_heat , pred_heat)     # unchanged
r2_cool   = r2_score(truth_cool , pred_cool)

# Build the metrics dict with these values
metrics = {
    "wandb_run":  WANB_RUN,
    "best_epoch": best_epoch + 1,
    "test_rmse_heating":  rmse_heat,
    "test_rmse_cooling":  rmse_cool,
    "test_mae_heating":   mae_heat,
    "test_mae_cooling":   mae_cool,
    "test_r2_heating":    r2_heat,
    "test_r2_cooling":    r2_cool,
    "test_mape_heating":  mape_heat,
    "test_mape_cooling":  mape_cool
}

# round all numeric metrics to max 5 decimal places
metrics_df = pd.DataFrame([metrics]).round(5)
metrics_df.to_csv(ERROR_OUT, index=False)

print("\nPredictions saved to:", PRED_PATH)
print("Best model saved to :", MODEL_PATH)
print("Error metrics saved to:", ERROR_OUT)
