deploy: 
	rm -rf dist/* 
	python -m build
	twine check dist/*
	twine upload dist/* 

docs-upload: 
	pdoc --docformat google -o docs vortex_api
	s3cmd sync docs/* s3://vortex-developers/docs/pyvortex/
	aws cloudfront create-invalidation --distribution-id E14G85YHOYJ3NL --paths "/docs/pyvortex/*"