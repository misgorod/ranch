dist/ranch:
	pyinstaller -F -n ranch --hidden-import scipy.interpolate --add-data ./funcs/lingers.npy:funcs --add-data ./funcs/batches.npy:funcs ranch/__main__.py

clean: dist/ranch
	rm -r dist/ranch
