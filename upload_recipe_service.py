import time
import uuid
from fractions import Fraction
import requests
import json
import pandas as pd
import sys
import argparse
import pymongo


class UploadRecipe:

    def __init__(self, path, collection):

        df = pd.read_csv(path, nrows=1000)
        self.recipes = df.to_dict(orient="records")
        self.db_collection = collection
        self.errors = []

    def clean_ingredient(self, detailed_ingredient):

        ingredient_name = detailed_ingredient[0]
        ingredient_unit = detailed_ingredient[1].split()
        ingredient_weight = detailed_ingredient[2].split()

        # Quantity and Unit
        if len(ingredient_unit) == 2:
            ingredient_quantity = float(sum(Fraction(s) for s in ingredient_unit[0].split()))
            ingredient_measure = ingredient_unit[1]

        elif len(ingredient_unit) >= 3:
            if ingredient_unit[1][0].isdigit():
                ingredient_quantity = float(sum(Fraction(s) for s in [ingredient_unit[0], ingredient_unit[1]]))
                ingredient_measure = " ".join(ingredient_unit[2:])
            else:
                ingredient_quantity = float(sum(Fraction(s) for s in ingredient_unit[0].split()))
                ingredient_measure = " ".join(ingredient_unit[1:])

        # Weight in grams
        if ingredient_weight[1] == "grams":
            ingredient_weight_grams = float(ingredient_weight[0])
        elif ingredient_weight[1] == "kg":
            ingredient_weight_grams = float(ingredient_weight[0]) * 1000
        else:
            print(f"Error in unit of weight for ingredient = {ingredient_name}")

        return {
            "ingredient_name": ingredient_name,
            "ingredient_quantity": ingredient_quantity,
            "ingredient_measure": ingredient_measure,
            "ingredient_weight_grams": ingredient_weight_grams
        }

    def split_ingredient(self, ingredient):

        # print(f"Cleaning  {ingredient}")
        ingredient_detailed = ingredient.split("#")
        # ingredient_detailed = []
        # curr = ""
        # for i, char in enumerate(ingredient):
        #     if char == "-":
        #         if ingredient[i + 1].isdigit():
        #             ingredient_detailed.append(curr)
        #             curr = ""
        #         else:
        #             curr += char
        #     else:
        #         curr += char
        # ingredient_detailed.append(curr)
        return self.clean_ingredient(ingredient_detailed)

    def get_ingredient_info(self, ingredient, response_dict):

        ingredient_cleaned = self.split_ingredient(ingredient)
        ingredient_name = ingredient_cleaned["ingredient_name"]
        ingredient_measure = ingredient_cleaned["ingredient_measure"]

        ingredient_weight_grams_scrapped = ingredient_cleaned["ingredient_weight_grams"]
        ingredient_quantity_scrapped = ingredient_cleaned["ingredient_quantity"]

        # print(f"Calling the API for {ingredient_name}")

        # calling the API to get the info
        search_response = requests.get(f"http://104.248.120.14:80/foods/search/{ingredient_name}").json()[0]
        nix_item_id = search_response["nix_item_id"]

        if nix_item_id is None:
            ingredient_info = requests.get(f"http://104.248.120.14:80/foods/common_food/{ingredient_name}").json()
        else:
            ingredient_info = requests.get(f"http://104.248.120.14:80/foods/branded_food/{nix_item_id}").json()

        random_id = str(uuid.uuid4())
        ingredient_info["id"] = random_id
        response_dict["nutrient_items"].append(ingredient_info)

        serving_wt_api = ingredient_info["serving_weight_grams"]

        # #new --------------------------------------
        # multiplication_factor = ingredient_weight_grams_scrapped / serving_wt_api
        # calories = ingredient_info["nutrients"]["calories"] * multiplication_factor
        # fat = ingredient_info["nutrients"]["total_fat"] * multiplication_factor
        # carbohydrate = ingredient_info["nutrients"]["total_carbohydrate"] * multiplication_factor
        # protein = ingredient_info["nutrients"]["protein"] * multiplication_factor
        # #end-new ----------------------------------


        wt_alt_measure = qty_alt_measure = -1
        for alt_measure in ingredient_info["alt_measures"]:
            if alt_measure["measure"] == ingredient_measure:
                wt_alt_measure = alt_measure["serving_weight"]
                qty_alt_measure = alt_measure["qty"]
                break
        # 1. Multiplication Factor will remain same because we are only concerned about the weight and we must use the wt. of the ingridient that we have scrapped.
        # 2. If we find the serving_unit that we have scrapped  ==  to the something in alt_measure then only we'll change the quantity, otherwise the quatity should
        #    be changed to the weight of the ingridient that we've scrapped. 
        # 3. The actual serving_wt should directly come from the scrapping.
        
        
        if wt_alt_measure == -1:
            multiplication_factor = ingredient_weight_grams_scrapped / serving_wt_api
        else:
            multiplication_factor = ingredient_weight_grams_scrapped / serving_wt_api
            new_quantity = (ingredient_weight_grams_scrapped * qty_alt_measure) / wt_alt_measure


        calories = ingredient_info["nutrients"]["calories"] * multiplication_factor
        fat = ingredient_info["nutrients"]["total_fat"] * multiplication_factor
        carbohydrate = ingredient_info["nutrients"]["total_carbohydrate"] * multiplication_factor
        protein = ingredient_info["nutrients"]["protein"] * multiplication_factor

        # print(f"Call done for {ingredient_name}")j
        return {
            'calories': calories,
            'fat': fat,
            'carbohydrate': carbohydrate,
            'protein': protein,
            "serving_wt": ingredient_weight_grams_scrapped,
            "serving_unit": "g" if wt_alt_measure == -1 else ingredient_measure,
            "id": random_id,
            "quantity": ingredient_weight_grams_scrapped if wt_alt_measure == -1 else new_quantity
        }

    def get_nutrition_info(self, ingredients, response_dict):
        nutrient_info = {'calories': 0, 'fat': 0, 'carbohydrate': 0, 'protein': 0, "serving_wt": 0}
        for ingredient in ingredients:
            info = self.get_ingredient_info(ingredient, response_dict)
            ingredient_dic = {
                "food_item_id": info["id"],
                "serving_unit": info["serving_unit"],
                "quantity": info["quantity"],
                "weight_grams": info["serving_wt"],
            }

            response_dict["ingredients"].append(ingredient_dic)

            nutrient_info["calories"] += info["calories"]
            nutrient_info["fat"] += info["fat"]
            nutrient_info["carbohydrate"] += info["carbohydrate"]
            nutrient_info["protein"] += info["protein"]
            nutrient_info["serving_wt"] += info["serving_wt"]

        return nutrient_info

    def create_recipe(self):

        print("\nExecution Started !")
        for recipe in self.recipes:
            time.sleep(5)
            try:
                print(f"Trying to upload {recipe['S.No']}")
                response_dict = {
                    "public_id": str(uuid.uuid4()),
                    "title": "",
                    "description": "-",
                    "image_url": "",
                    "video_url": "",
                    "owner_id": "OE2SShWQBNS75KmN9DLHsGWqF9W2",
                    "cooking_time": 0,
                    "serving_unit": "serving",
                    "quantity": 1,
                    "meal_type": "",
                    "tenant_id": "trainergoesonline",
                    "ingredients": [],
                    "tags": [],
                    "nutrient_items": [],
                    "nutrients": {
                        "calories": 0,
                        "carbohydrates": 0,
                        "fats": 0,
                        "proteins": 0
                    },
                    "instructions": [],
                    "serving_weight_grams": 0,
                    # "common": False,
                    # "deleted": False,
                    # "created_at": {
                    #     "$date": {
                    #         "$numberLong": ""
                    #     },
                    #     "updated_at": {
                    #         "$date": {
                    #             "$numberLong": ""
                    #         }
                    #     },
                    # }
                }

                # title
                response_dict["title"] = recipe["Recipe_title"]

                # tags
                tags = recipe["Recipe_title"].split()
                for tag in tags:
                    response_dict["tags"].append(tag.lower())

                # cooking time
                time_min = recipe["Req_Time"].split(',')
                time_sec = int(float(time_min[0]) * 60 + float(time_min[1]) * 60)
                response_dict["cooking_time"] = time_sec

                #  cooking instructions
                instruction_lst = recipe["Instruction_to_cook"].split("Step-")
                for instruction in instruction_lst:
                    if instruction:
                        response_dict['instructions'].append(instruction.strip())

                # image url
                response_dict["image_url"] = recipe["Image_src"]

                # meal type
                meals = str(recipe["B/L/S/D"])
                meals = "0"*(4-len(meals))+meals
                binary_meal_type = ",".join(list(meals))
                response_dict["meal_type"] = binary_meal_type

                # if recipe["Meal_Type"] == "Breakfast":
                #     response_dict["meal_type"] = "0"
                # elif recipe["Meal_Type"] == "Lunch":
                #     response_dict["meal_type"] = "1"
                # elif recipe["Meal_Type"] == "Dinner":
                #     response_dict["meal_type"] = "2"
                # elif recipe["Meal_Type"] == "Snack":
                #     response_dict["meal_type"] = "3"
                # else:
                #     print("Meal type not defined")


                # Nutrients list, nutrients and ingredients
                s = recipe["Ingredients_list"]
                # s = s.replace("grams,", "grams|")
                # s = s.replace("kg,", "kg|")
                ingredients = s.split('|')

                nutrients = self.get_nutrition_info(ingredients, response_dict)
                response_dict["nutrients"]["proteins"] = nutrients["protein"]
                response_dict["nutrients"]["fats"] = nutrients["fat"]
                response_dict["nutrients"]["carbohydrates"] = nutrients["carbohydrate"]
                response_dict["nutrients"]["calories"] = nutrients["calories"]

                response_dict["serving_weight_grams"] = nutrients["serving_wt"]

                # posting the data to the api
                # url = "http://104.248.120.14:80/recipe"
                # x = requests.post(url, json=response_dict)

                # printing the json object
                # json_object = json.dumps(response_dict, indent=4)
                # print("\n")
                # print(json_object)

                # uploading to the local mongodb
                self.db_collection.insert_one(response_dict)
                print("Uploaded Successfully!")
            except Exception as e:
                print("NOT UPLOADED DUE TO SOME ERROR")
                self.errors.append([recipe['S.No'], recipe["Link"], e])

        for sno, link, error in self.errors:
            print(sno, link, error)





def main(path):
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["tgo"]
    collection = db["corrected_final"]
    obj = UploadRecipe(path, collection)
    obj.create_recipe()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--scrapped_file_path', type=str, required=True,
                        help="input path of the scrapped recipes to be uploaded")
    args = parser.parse_args()
    path = args.scrapped_file_path
    main(path)
