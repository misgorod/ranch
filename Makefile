dist/ranch:
	pyinstaller -F -n ranch --hidden-import scipy.interpolate --add-data ./lingers.npy:funcs --add-data ./batches.npy:funcs ranch/__main__.py

clean: dist/ranch
	rm -r dist/ranch
