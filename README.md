# 🚲 Bike Sharing Datasets

This repository provides tools to download, process, and structure bike-sharing datasets from multiple cities for analysis and research purposes.

## 📥 Datasets Download

You can download bike-sharing datasets from the following sources:

- **New York**: [CitiBike](https://ride.citibikenyc.com/system-data)
- **Washington**: [Capital Bikeshare](https://www.capitalbikeshare.com/system-data)
- **Bay Area**: [BayWheels](https://www.lyft.com/bikes/bay-wheels/system-data)
- **Columbus**: [CoGo](https://www.cogobikeshare.com/system-data)
- **Boston**: [Blue Bikes](https://www.bluebikes.com/system-data)

## ⚙️ Installation

Clone the repository and install dependencies:

```sh
git clone <repo_link>
cd <repo_directory>
pip install -r requirements.txt
```

## 📊 Dataset Pipeline Overview

The dataset pipeline consists of two main stages:
1. **Data Extraction**: Downloads and preprocesses raw data.
2. **Sub-dataset Building**: Constructs train and evaluation datasets from raw data.

---

### 🔍 Data Extraction

This stage includes:

1. **`downloader`**: Downloads raw trace files from the sources.
2. **`split`**: Splits the dataset into smaller chunks for efficient processing.
3. **`docking`**: Extracts unique docking stations.
4. **`raw`**: Extracts and stores trips in raw format (start station, end station, time, duration).

Each step can be executed independently using:

```sh
python main.py <command_name>
```

To execute all extraction steps for a given year:

```sh
python main.py all 2022
```

#### 🌦️ Extracting Weather Data

To fetch weather data for a specific time range:

```sh
python main.py weather new_york 2022-03-01 2022-09-01 --collection-name observations
```

---

### 🏗️ Sub-dataset Building

This step constructs the final dataset from raw trips. The `subdataset` command is used for this purpose.

#### 📌 Train Dataset Command

```sh
python main.py subdataset "none" 10,20,40 data/dataset citibike --name-suffix training --min-date 2022-03-01 --max-date 2022-08-01
```

#### 🎯 Evaluation Dataset Command

```sh
python main.py subdataset "none" 10,20,40 data/dataset citibike --name-suffix evaluation --min-date 2022-08-01 --max-date 2022-09-01
```

---

## 🌍 Full Dataset with Zones

1. Generate zones:

```sh
python main.py zones 51 34 data/zones/ny --filter-region 71
```

2. Use zones to create the train dataset:

```sh
python main.py subdataset "39,0,28,41,31" 5-zones data/dataset-zones citibike --name-suffix training --min-date 2022-03-01 --max-date 2022-08-01 --nodes-from-zones --zones-path data/zones/ny/zones.json --add-weather-data --weather-db weather-new_york --weather-collection observations
```

3. Generate the evaluation dataset:

```sh
python main.py subdataset "39,0,28,41,31" 5-zones data/dataset-zones citibike --name-suffix evaluation --min-date 2022-08-01 --max-date 2022-09-01 --nodes-from-zones --zones-path data/zones/ny/zones.json --add-weather-data --weather-db weather-new_york --weather-collection observations
```

---

## 📌 CDRC Dataset

To access CDRC bike-sharing data, apply at [CDRC](https://www.cdrc.ac.uk/) for access to [meddin-bike-sharing-world-map-data](https://data.cdrc.ac.uk/dataset/meddin-bike-sharing-world-map-data) for the following cities:

- **Dublin**
- **London**
- **Paris**
- **New York**

### 📂 CDRC Dataset Structure

Each city dataset consists of the following files:

1. `<city>_bikelocations.csv` - Docking station details, including location changes over time.
2. `<city>_ind_<year>.csv` - 10-minute interval observations.
3. `<city>_ind_hist.csv` - Historical observations (similar to file 2).
4. `<city>_sum.csv` - Aggregated system statistics (every 10 minutes).

🔹 **Identifiers:** `ucl_id` and `tfl_id` are unique across files.

### 🏗️ CDRC Data Processing Pipeline

The CDRC processing pipeline consists of:

1. **`docking`** - Extracts unique docking stations.
2. **`raw`** - Extracts and stores raw trips.
3. **`zones`** - Creates geographical zones.
4. **`subdataset`** - Merges raw data, docking station info, and zones.

The entire pipeline can be executed with:

```sh
python main.py --cdrc all all <city> --dataset-path data/cdrc_data/datasets --nodes-from-zones --min-date <start_date> --max-date <end_date> --name-suffix <suffix>
```

#### 🇮🇪 Dublin Dataset

**Training:**
```sh
python main.py --cdrc all all dublin --dataset-path data/cdrc_data/datasets --nodes-from-zones --min-date 2021-09-01 --max-date 2022-08-01 --name-suffix training
```

**Evaluation:**
```sh
python main.py --cdrc all all dublin --dataset-path data/cdrc_data/datasets --nodes-from-zones --min-date 2022-08-01 --max-date 2022-09-01 --name-suffix evaluation --skip docking,raw,zones
```

#### 🇬🇧 London Dataset

**Training:**
```sh
python main.py --cdrc all all london --dataset-path data/cdrc_data/datasets --nodes-from-zones --min-date 2021-09-01 --max-date 2022-08-01 --name-suffix training
```

**Evaluation:**
```sh
python main.py --cdrc all all london --dataset-path data/cdrc_data/datasets --nodes-from-zones --min-date 2022-08-01 --max-date 2022-09-01 --name-suffix evaluation --skip docking,raw,zones
```

---

## 🎯 Summary

This repository streamlines bike-sharing dataset processing for research and analysis. It supports multiple cities, integrates weather data, and allows for structured dataset generation for training and evaluation.

For further details, run:
```sh
python main.py --help
```
