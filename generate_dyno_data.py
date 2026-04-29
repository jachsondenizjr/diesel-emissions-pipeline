from pipeline.generator import DynoDataGenerator
from pipeline.settings import load_settings

if __name__ == "__main__":
    settings = load_settings()
    DynoDataGenerator(settings).run()
