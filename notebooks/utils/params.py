#These variables will 99% stay the same
URL_PREFIX = 'https://archive.org/download/stackexchange/'
URL_SUFFIX = '.stackexchange.com.7z'
TABLE_NAMES = ['badges', 'comments', 'posthistory', 'posts', 'tags', 'users', 'votes']
BRONZE_DIR = '/dbfs/mnt/stackoverflow/bronze'

#TODO change depending on which categories you're interested in
URL_LIST = ['beer', 'vegetarianism']

#TODO take this from your Azure setup
STORAGE_ACCOUNT = 'stackoverflowstorage'
APPLICATION_ID = 'fff8eed9-4e4a-41ad-be4c-ba560a8698bb' # get it from app registration
DIRECTORY_ID = '49c2075d-fa21-4c06-b6b9-6802bf10b0bb' # get it from app registration
