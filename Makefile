deploy: 
	rm -rf dist/* 
	python -m build
	twine upload dist/* 

docs: 
	pdoc --docformat google -o docs vortex_api