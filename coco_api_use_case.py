"""
coco API use case.

Created On 9th Mar, 2020
Author: bohang.li
"""
import os
import torch
from torch.utils.data import Dataset, DataLoader
from collections import Counter
from torchvision import datasets
from pycocotools.coco import COCO
import skimage.io as sio
import pandas as pd


image_folder = "/ldap_home/bohang.li/image_search_download/download"
jsonfile = "/ldap_home/bohang.li/image_search_download/image_search_data_annotation.json"
# jsonfile = "/ldap_home/bohang.li/image_search_download/image_search_val.json"
category_list = [str(x) for x in category_list]
inv_cat_idx = {k: v for v, k in enumerate(category_list)}
coco_dict = COCO(jsonfile)
img2anns = coco_dict.imgToAnns
samples = []
count = 0
for img, labels in img2anns.items():
    cat_ids = set()
    for lbl in labels:
        cat_ids.add(lbl["category_id"])
    # remove muti-label images(different labels), only reduce 2168466-2168449=17 images.
    if len(cat_ids) > 1 or len(cat_ids) <= 0:
        continue
    category = coco_dict.cats[list(cat_ids)[0]]["name"]
    # remove class not in our list.
    if category not in category_list:
        continue
    img_filename = coco_dict.imgs[img]["file_name"]
    samples.append({"id": str(count).zfill(8), "image": img_filename.split(".")[0], "label": category})
    count += 1

res = pd.DataFrame(samples)
res.to_csv("../embedding.csv", index=False)


class ImageSearchDataset(Dataset):
    """
    Dataset for internal image search dataset.
    """
    def __init__(self, jsonfile, category_list, image_folder, transform=None, target_transform=None):
        """

        :param jsonfile: coco format json file.
        :param category_list: list, categories needed.
        :param image_folder: str, the image folder path.
        """
        category_list = [str(x) for x in category_list]
        self.image_folder = image_folder
        self.category_list = category_list
        self.transform = transform
        self.target_transform = target_transform
        coco_dict = COCO(jsonfile)
        img2anns = coco_dict.imgToAnns
        samples = []
        for img, labels in img2anns.items():
            cat_ids = set()
            for lbl in labels:
                cat_ids.add(lbl["category_id"])
            # remove muti-label images(different labels), only reduce 2168466-2168449=17 images.
            if len(cat_ids) > 1 or len(cat_ids) <= 0:
                continue
            category = coco_dict.cats[list(cat_ids)[0]]["name"]
            # remove class not in our list.
            if category not in category_list:
                continue
            img_filename = coco_dict.imgs[img]["file_name"]
            samples.append({"image": img_filename, "label": self.inv_cat_idx[category]})
        self.samples = samples

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, item):
        img_file, label = self.samples[item]["image"], self.samples[item]["label"]
        image = sio.imread(os.path.join(self.image_folder, img_file))
        if self.transform is not None:
            image = self.transform(image)
        if self.target_transform is not None:
            label = self.target_transform(label)
        return image, label

    @property
    def cat_idx(self):
        idx_dict = {k: v for k, v in enumerate(self.category_list)}
        return idx_dict

    @property
    def inv_cat_idx(self):
        inv_cat_idx = {k: v for v, k in enumerate(self.category_list)}
        return inv_cat_idx

    def stats(self):
        c = Counter()
        for item in self.samples:
            c[self.cat_idx[item["label"]]] += 1
        return c
