def calculate_sndhwm(buffer_size_gb, message_size_bytes):
    """
    Calculate the SNDHWM (high water mark for outbound messages) based on the buffer size and message size.

    Parameters:
    buffer_size_gb (float): The size of the buffer in gigabytes.
    message_size_bytes (int): The size of a single message in bytes.

    Returns:
    int: The calculated SNDHWM value.
    """
    # Convert buffer size from gigabytes to bytes (1 GB = 1024^3 bytes)
    buffer_size_bytes = buffer_size_gb * (1024**3)

    # Calculate SNDHWM as the number of messages that fit into the buffer
    sndhwm = buffer_size_bytes // message_size_bytes

    return int(sndhwm)

# Example usage:
# Assuming you have a buffer of 50 GB and the message size is 256 bytes
buffer_size = 50  # GB
message_size = 2097152*4  # bytes
sndhwm = calculate_sndhwm(buffer_size, message_size)
print(f"The SNDHWM based on a message size of {message_size} bytes is: {sndhwm}")
