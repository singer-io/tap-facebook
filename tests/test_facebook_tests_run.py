import os

potential_paths = [
    'tests/',
    '../tests/'
    'tap-facebook-square/tests/',
    '../tap-facebook/tests/',
]


def go_to_tests_directory():
    for path in potential_paths:
        if os.path.exists(path):
            os.chdir(path)
            return os.getcwd()
    raise NotImplementedError("This check cannot run from {}".format(os.getcwd()))

##########################################################################
### TEST
##########################################################################


print("Acquiring path to tests directory.")
cwd = go_to_tests_directory()

print("Reading in filenames from tests directory.")
files_in_dir = os.listdir(cwd)

print("Dropping files that are not of the form 'test_<feature>.py'.")
test_files_in_dir = [fn for fn in files_in_dir if fn.startswith('test_') and fn.endswith('.py')]

print("Files found: {}".format(test_files_in_dir))

print("Reading contents of circle config.")
with open(cwd + "/../.circleci/config.yml", "r") as config:
    contents = config.read()

print("Parsing circle config for run blocks.")
runs = contents.replace(' ', '').replace('\n', '').split('-run_integration_tests:')

print("Verify all test files are executed in circle...")
tests_not_found = set(test_files_in_dir)
for filename in test_files_in_dir:
    print("\tVerifying {} is running in circle.".format(filename))
    if any([filename in run for run in runs]):
        tests_not_found.remove(filename)
assert tests_not_found == set(), "The following tests are not running in circle:\t{}".format(tests_not_found)
print("\t SUCCESS: All tests are running in circle.")
