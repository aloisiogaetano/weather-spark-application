import sys
from jobs.clear_days import run as run_clear_days
from jobs.nation_stats import run as run_nation_stats
from jobs.avg_temperature_diff import run as run_avg_temperature_diff

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <task>")
        print("Available tasks: task1, task2, task3")
        sys.exit(1)
    task = sys.argv[1].lower()
    if task == "task1":
        run_clear_days("data/raw/weather_description.csv")
    elif task == "task2":
        run_nation_stats(
            "data/raw/temperature.csv",
            "data/raw/pressure.csv",
            "data/raw/humidity.csv",
            "data/raw/city_attributes.csv"
        )
    elif task == "task3":
        run_avg_temperature_diff(
            "data/raw/temperature.csv",
            "data/raw/city_attributes.csv"
        )
    else:
        print(f"Unknown task: {task}")
        sys.exit(1)
if __name__ == "__main__":
    main()