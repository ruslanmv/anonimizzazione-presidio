# Configure training for spacy model

Make sure that you have spacy and the model already installed

In the base_config.cfg file:

1. Change the value of _vectors_ with the name of the model that you want to train
2. Change the value of _lang_ to the correct language: **must** be the same of the model

Then give this command:

```bash
python -m spacy init fill-config ./base_config.cfg ./config.cfg
```

Now you have the config file for training the model. Make sure to change the following value:

1. The value of _train_ to the directory that contains the training datasets
2. The value of _dev_ to the directory that contains the testing datasets
   If your dataset is not in the .spacy format then use the following command to convert it:

```bash
python -m spacy convert <input_file> <output_dir>
```

Give then this command to initialize training:

```bash
python -m spacy train config.cfg --output ./output
```
