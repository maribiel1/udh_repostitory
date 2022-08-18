""" 
Methods for changing UDH PGS metadata
"""
import logging
from typing import NamedTuple

import pandas as pd


class InventoryFileConfig(NamedTuple):
    """
    Class for inventory metadata configuration
    """

    inventory_meta_location: str
    inventory_worksheet_name: str
    inventory_sheet_columns: list
    inv_file_path: str
    inv_file_name: str
    inv_udh_layer: str
    inv_namespace: str
    inv_item: str
    inv_snowflake_table_schema: str
    inv_snowflake_object_name: str
    inv_ready_to_snowflake_publish: str


class TargetFileConfig(NamedTuple):
    """
    Class for the configuration of the target BCG data file
    """

    target_workbook_file: str
    target_worksheet_name: str
    target_sheet_columns: list
    publish_type: str
    publish_target_connection: str
    publish_custome_mode: str
    publish_mode: str


class UdhRepositoryTransform:
    """
    Read inventory file, transform UDH Code and writes transormed code
    """

    def __init__(self, inv_args: InventoryFileConfig, trg_agrs: TargetFileConfig):
        """
        Constructor for UDH PGS transformer
        """
        self._logger = logging.getLogger(__name__)
        self.inv_args = inv_args
        self.trg_agrs = trg_agrs

    def load_inventory_df(self):
        """
        Load inventory xlsx file with necessary columns

        :returns:
            inventory_df: Pandas DataFrame with the extracted data from inventory excel file
        """
        self._logger.info("Loading inventory data started...")
        inventory_df = pd.read_excel(
            self.inv_args.inventory_meta_location,
            sheet_name=self.inv_args.inventory_worksheet_name,
        )

        # Filter necessary inventory columns
        inventory_df = inventory_df.loc[:, self.inv_args.inventory_sheet_columns]
        self._logger.info("Loading inventory data finished.")
        return inventory_df

    def map_inventory(self) -> dict:
        """
        Select and Map inventory metadata into output bcg structure

        :returns:
            mappings: Python dictionary with output bcg mapping
        """
        self._logger.info("Mapping inventory to BCG entries started...")
        inventory_df = self.load_inventory_df()
        mappings = {}
        inventory_df.reset_index()
        # Iterate rows
        for index, row in inventory_df.iterrows():
            mappings[row["Path"] + "/" + row["File_name"]] = {
                "LAYER": row[self.inv_args.inv_udh_layer],
                "NAMESPACE": row[self.inv_args.inv_namespace],
                "DATA_ITEM": row[self.inv_args.inv_item],
                "PUBLISH_TARGET_OBJECT_SCHEMA": row[
                    self.inv_args.inv_snowflake_table_schema
                ],
                "PUBLISH_TARGET_OBJECT_TABLE": row[
                    self.inv_args.inv_snowflake_object_name
                ],
                "Ready": row[self.inv_args.inv_ready_to_snowflake_publish],
            }
        self._logger.info("Mapping inventory to BCG entries completed")
        return mappings

    def create_bcg_df(self) -> pd.DataFrame:
        """
        Create bcg metadata

        :returns:
            bcg_df: Pandas DataFrame with the extracted data from inventory excel file
        """
        self._logger.info("Creating data frame with BCG entries started...")
        mappings = self.map_inventory()
        bcg_df = pd.DataFrame(columns=self.trg_agrs.target_sheet_columns)

        for k, v in mappings.items():
            Ready = mappings[k]["Ready"]
            LAYER = mappings[k]["LAYER"]
            NAMESPACE = mappings[k]["NAMESPACE"]
            DATA_ITEM = mappings[k]["DATA_ITEM"]
            PUBLISH_TARGET_OBJECT_SCHEMA = mappings[k]["PUBLISH_TARGET_OBJECT_SCHEMA"]
            PUBLISH_TARGET_OBJECT_TABLE = mappings[k]["PUBLISH_TARGET_OBJECT_TABLE"]

            if Ready == 1.0:
                if (
                    str(PUBLISH_TARGET_OBJECT_SCHEMA) == "nan"
                    or str(PUBLISH_TARGET_OBJECT_TABLE) == "nan"
                ):
                    print(
                        f'\n[{k}]: Missing information in Redshift_to_Snowflake_Inventory: "Snowflake table schema" or/and "Snowflake Object Name"\n'
                    )
                else:
                    PUBLISH_TARGET_OBJECT = (
                        PUBLISH_TARGET_OBJECT_SCHEMA + "." + PUBLISH_TARGET_OBJECT_TABLE
                    )
                    record = (
                        LAYER,
                        NAMESPACE,
                        DATA_ITEM,
                        self.trg_agrs.publish_type,
                        self.trg_agrs.publish_target_connection,
                        PUBLISH_TARGET_OBJECT,
                        self.trg_agrs.publish_custome_mode,
                        self.trg_agrs.publish_mode,
                    )
                    bcg_df.loc[len(bcg_df)] = record
                    # data.append(record)
        self._logger.info("Creating data framme with BCG entries completed")
        return bcg_df

    def create_bcg_entries(self):
        """
        Save BCG entry in excel file
        """
        data = self.create_bcg_df()
        data.to_excel(
            self.trg_agrs.target_workbook_file,
            sheet_name=self.trg_agrs.target_worksheet_name,
            index=False,
        )

        return True

    def load_json(self):
        """
        Load json file
        """

    def update_json_files(self):
        """
        Update json files
        """
        self._logger.info("Updating BCG json files started...")
        mappings = self.map_inventory()
        bcg_df = pd.DataFrame(columns=self.trg_agrs.target_sheet_columns)
