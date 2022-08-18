import argparse
import logging
import logging.config

import yaml

from metadata.transform.transformer import (
    InventoryFileConfig,
    TargetFileConfig,
    UdhRepositoryTransform,
)


def main():
    """
    Entry point to start udh code change process
    """
    # Parsing YAML file
    parser = argparse.ArgumentParser(description="Run the .")
    parser.add_argument("config", help="A configuration file in YAML format.")
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))

    # configure logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    # reading configuration
    inventory_config = InventoryFileConfig(**config["inventory"])
    target_config = TargetFileConfig(**config["target_file_config"])

    # running generating bcg entries
    logger.info("Generating BCG entries started")
    udh_transform = UdhRepositoryTransform(inventory_config, target_config)
    udh_transform.create_bcg_entries()
    logger.info("Generating BCG entries finished.")


if __name__ == "__main__":
    main()
