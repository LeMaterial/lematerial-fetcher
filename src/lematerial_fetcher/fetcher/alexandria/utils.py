import os

from lematerial_fetcher.utils.logging import logger


def sanitize_json(obj):
    """Replace NaN values with null in JSON data."""
    if isinstance(obj, dict):
        return {k: sanitize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_json(x) for x in obj]
    elif isinstance(obj, float) and (str(obj) == "nan" or str(obj) == "NaN"):
        return None
    return obj


def replace_nan_in_large_json(
    input_filepath: str, output_filepath: str, chunk_size: int = 1024 * 1024
) -> str:
    """Replaces NaN values with null in large JSON files efficiently.

    Parameters
    ----------
    input_filepath : str
        Path to the input JSON file.
    output_filepath : str
        Path to the output JSON file.
    chunk_size : int, optional
        The size of chunks (in bytes) to read from the file.
        Defaults to 1MB. Adjust based on available memory.

    Returns
    -------
    str
        The path to the output JSON file.

    Notes
    -----
    This function streams the file and performs text replacement.
    It assumes 'NaN' appears only as a numeric literal (like Infinity or -Infinity)
    and NOT inside string literals (e.g., "key": "contains NaN").
    If 'NaN' can appear inside strings, this function will incorrectly replace it.
    """
    try:
        processed_size = 0

        with (
            open(input_filepath, "r", encoding="utf-8", errors="replace") as f_in,
            open(output_filepath, "w", encoding="utf-8") as f_out,
        ):
            carry_over = ""  # Stores potential trailing part of NaN from previous chunk
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    # End of file
                    # Process any remaining carry_over
                    if carry_over:
                        modified_carry = carry_over.replace("NaN", "null")
                        f_out.write(modified_carry)
                    break

                current_data = carry_over + chunk
                chunk_size_actual = len(chunk)
                processed_size += chunk_size_actual

                # --- Boundary Handling ---
                # Check if the chunk might end mid-'NaN'
                # We need to look back up to 2 characters (for 'Na')
                if current_data.endswith("Na"):
                    process_part = current_data[:-2]
                    carry_over = "Na"
                elif current_data.endswith("N"):
                    process_part = current_data[:-1]
                    carry_over = "N"
                else:
                    # No potential split 'NaN' at the end
                    process_part = current_data
                    carry_over = ""

                modified_chunk = process_part.replace("NaN", "null")

                f_out.write(modified_chunk)

        return output_filepath

    except FileNotFoundError:
        logger.error(f"Error: Input file not found at '{input_filepath}'")
        raise FileNotFoundError(f"Input file not found at '{input_filepath}'")
    except IOError as e:
        logger.error(f"Error processing file: {e}")
        raise IOError(f"Error processing file: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        # Clean up potentially incomplete output file on unexpected errors
        if os.path.exists(output_filepath):
            try:
                os.remove(output_filepath)
                logger.info(f"Cleaned up incomplete output file: '{output_filepath}'")
            except OSError as rm_err:
                logger.error(
                    f"Error cleaning up output file '{output_filepath}': {rm_err}"
                )
