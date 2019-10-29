class Movie:

    def __init__(self,title,original_title,duration,release_date,rating ):
        self.title = title
        self.original_title = original_title
        self.duration = duration
        self.release_date = release_date
        self.rating = rating

        self.id = None
        self.actors = [] #Pas en argument de la méthode init donc non obligatoire, création d'un tableau vide pour les acteurs
        self.productors = []
        self.is_3d = None #None = je ne sais pas
        self.production_budget = None
        self.marketing_budget = None
    
    def total_budget(self):
        if (self.production_budget == None or self.marketing_budget == None):
            return None

        return self.production_budget + self.marketing_budget
        