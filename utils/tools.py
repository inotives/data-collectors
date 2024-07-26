import hashlib 

def generate_unique_key(*args):
    """
    Generates a unique key using the provided variables.
    Args:
        *args: Any number of variables to be used in the key generation.
    Returns:
        A unique key as a hexadecimal string.
    """
    # Concatenate string representations of all arguments
    concatenated_string = ''.join(map(str, args))
    
    # Generate a unique key using SHA-256 hash
    unique_key = hashlib.sha256(concatenated_string.encode()).hexdigest()
    
    return unique_key
