# Refactored out of s3_syncer.py to reduce transitive dependencies.

def generate_s3_key_name_from_image_name(image_file_name):
    """
    Generate image s3_key_name based on input image_file_name
    >>> generate_s3_key_name_from_image_name('3ff1fbe10f7935e9-0-0.jpg')
    3ff/fb/3ff1fbe10f7935e9-0-0.jpg
    """
    trimmed_file_name = image_file_name
    parts = image_file_name.split('.')
    if len(parts) > 1 and parts[0] == 'h':
        trimmed_file_name = '.'.join(parts[1:])
    return trimmed_file_name[0:3] + '/' + trimmed_file_name[4:6] + '/' + image_file_name


def image_file_key_name_gen(file_path):
    """Get the s3 key for the file.
    >>> image_file_key_name_gen('/home/clipu/c2/imgs/3ff/fb/3ff1fbe10f7935e9-0-0.jpg')
    '3ff/fb/3ff1fbe10f7935e9-0-0.jpg'
    """
    return '/'.join(file_path.split('/')[-3:])


def image_file_prefix_gen(file_path):
    """Get the s3 bucket key prefix for the file.
    >>> image_file_prefix_gen('/home/clipu/c2/imgs/3ff/fb/3ff1fbe10f7935e9-0-0.jpg')
    '3ff'
    """
    return '/'.join(file_path.split('/')[-3:-2])


def canonicalize_platform_bucket_name(platform_bucket_name):
    # default platform prefix is 's3:', in case it's not provided.
    if platform_bucket_name.find(':') == -1:
        return 's3:' + platform_bucket_name
    return platform_bucket_name


def get_platform_from_bucket_name(platform_bucket_name):
    parts = platform_bucket_name.split(':')
    if len(parts) == 1:
        # If platform_bucket_name prefix is not provided, fall back to default 's3'.
        return 's3'
    return parts[0]


def extract_bucket_name(platform_bucket_name):
    return platform_bucket_name.split(':')[-1]
