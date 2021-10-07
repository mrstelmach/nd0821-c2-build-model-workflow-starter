#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os
import tempfile

import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    logger.info(f'Using {artifact_local_path} artifact to read with pandas')
    df = pd.read_csv(artifact_local_path)
    
    logger.info('Dropping outliers outside the minimum and maximum price range')
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    logger.info('Converting last_review to datetime type')
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    logger.info(f'Saving {args.output_artifact} artifact')
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_path = os.path.join(tmp_dir, "clean_sample.csv")
        df.to_csv(file_path, index=False)
        
        artifact = wandb.Artifact(
            name=args.output_artifact, 
            type=args.output_type,
            description=args.output_description
        )
        artifact.add_file(file_path)
        run.log_artifact(artifact)
        artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact name",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
