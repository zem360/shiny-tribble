def load_data(df, output_path, file_format="parquet", mode="overwrite"):
    """
    Writes the final DataFrame to the specified destination.
    """
    writer = df.write.mode(mode)
    
    if file_format.lower() == "parquet":
        writer.parquet(output_path)
    elif file_format.lower() == "csv":
        writer.option("header", "true").csv(output_path)
    else:
        raise ValueError("Unsupported format. Please use 'parquet' or 'csv'.")
    
    print(f"Data successfully saved to {output_path} in {file_format} format.")
