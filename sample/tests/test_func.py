from app.app import some_function
def test_some_function():
    nums = [i for i in range(11)]
    assert some_function(*nums) == 55