#!/usr/bin/env python
# coding: utf-8

# In[1]:


import atoti as tt


# In[2]:


session = tt.Session(user_content_storage="content", java_options=["-Xmx8G"])
session.link()


# In[3]:


#!conda install -c conda-forge python-wget -y


# In[4]:


from zipfile import ZipFile

import wget
from IPython.display import clear_output, display


# In[5]:


def bar_custom(current, total, width=80):
    clear_output(wait=True)
    print("Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total))


url = "https://s3.eu-west-3.amazonaws.com/data.atoti.io/notebooks/ifrs9/lending-club-data.zip"
filename = wget.download(url, bar=bar_custom)


# In[6]:


# unzipping the file
with ZipFile("lending-club-data.zip", "r") as zipObj:
    # Extract all the contents of zip file in current directory
    zipObj.extractall()


# In[7]:


src = "./"


# In[8]:


risk_engine_data = session.read_csv(
    src + "lending-club-data/risk-engine/*.csv",
    keys=["Reporting Date", "id"],
    table_name="Credit Risk",
    types={
        "EAD": tt.type.DOUBLE,
        "Stage": tt.type.INT,
        "Previous Stage": tt.type.INT,
        "DaysPastDue": tt.type.DOUBLE,
    },
    default_values={"Stage": 0, "Months Since Inception": 0},
)
risk_engine_data.head(3)


# In[9]:


cube = session.create_cube(risk_engine_data, "IFRS9")


# In[10]:


lending_club_data = session.read_csv(
    src + "lending-club-data/loans.csv",
    keys=["id"],
    table_name="Lending Club Data",
    process_quotes=True,
    default_values={
        "Opening Year": 1970,
        "Opening Month": 1,
        "Opening Day": 1,
    },
)
risk_engine_data.join(lending_club_data)
lending_club_data.head(3)


# In[11]:


loans_at_inception = session.read_csv(
    "lending-club-data/static.csv",
    keys=["id"],
    table_name="Parameters at inception",
)
loans_at_inception.head(3)


# In[12]:


# Linking contracts and their opening information
risk_engine_data.join(loans_at_inception)


# In[13]:


cube.schema


# In[14]:


# These are the variables for faster access to the cube data elements:
l, m, h = cube.levels, cube.measures, cube.hierarchies


# In[15]:


# By setting the sort on the reporting date to DESC, we make sure that the latest date appears by default.
l["Reporting Date"].order = tt.NaturalOrder(ascending=False)


# In[16]:


# Multi-level hierarchies come handy when you have a typical way to expand data:
h["Opening Date (detailed)"] = {
    "Year": lending_club_data["Opening Year"],
    "Month": lending_club_data["Opening Month"],
    "Day": lending_club_data["Opening Day"],
}


# In[17]:


# with 0.6.0, int and long table columns, unless they are key columns, automatically become measures instead of levels.
# With this change, all the numeric columns behave the same.

h["Stage"] = [risk_engine_data["Stage"]]
h["Months Since Inception"] = [risk_engine_data["Months Since Inception"]]


# Measures visualizing credit risk inputs

# In[18]:


m["Stage"] = tt.agg.single_value(risk_engine_data["Stage"])
m["Stage"].folder = "Stage"

# This is how we can create measures to display the previous value and daily changes side by side with the value:
m["Previous Stage"] = tt.shift(m["Stage"], h["Reporting Date"], offset=1)
m["Previous Stage"].folder = "Stage"

m["Stage Variation"] = tt.where(
    ~m["Previous Stage"].isnull(), m["Stage"] - m["Previous Stage"]
)

m["Stage Variation"].folder = "Stage"
# https://github.com/activeviam/atoti/issues/3820
m["Stage Variation"].formatter = "DOUBLE[+#,###;-#,###]"


# EAD aggregation
# The Exposure At Default is the estimated amount of loss a bank may be exposed to when a debtor defaults on a loan.

# In[19]:


m["EAD"] = tt.agg.sum(risk_engine_data["EAD"])
m["EAD"].folder = "EAD"

# This is how we can create measures to display the previous value and daily changes side by side with the value:
m["Previous EAD"] = tt.shift(m["EAD"], h["Reporting Date"], offset=1)
m["Previous EAD"].folder = "EAD"

m["EAD (Chg)"] = tt.where(~m["Previous EAD"].isnull(), m["EAD"] - m["Previous EAD"])
m["EAD (Chg)"].folder = "EAD"
m["EAD (Chg)"].formatter = "DOUBLE[+#,###.##;-#,###.##]"

m["EAD (Chg %)"] = tt.where(m["Previous EAD"] != 0, m["EAD (Chg)"] / m["Previous EAD"])
m["EAD (Chg %)"].folder = "EAD"
m["EAD (Chg %)"].formatter = "DOUBLE[+#,###.##%;-#,###.##%]"


# Here's a query for the EAD and variations:

# In[20]:


cube.query(
    m["EAD"],
    m["Previous EAD"],
    m["EAD (Chg)"],
    m["EAD (Chg %)"],
    levels=[l["Reporting Date"]],
)


# Visualizing PD
# 
# 
# 
# 
# The Probability Of Default is the likelihood that your debtor will default on its debts (goes bankrupt or so) within certain period (12 months for loans in Stage 1 and life-time for other loans).
# 
# PD (12) - is 12 month probability of default and PD (LT) - is the lifetime probability of default.
# 
# 12 months PD and variations

# In[21]:


m["PD (12)"] = tt.agg.mean(risk_engine_data["PD12"])
m["PD (12)"].folder = "PD"
m["PD (12)"].formatter = "DOUBLE[#,###.##%]"

m["Previous PD (12)"] = tt.shift(m["PD (12)"], h["Reporting Date"], offset=1)
m["Previous PD (12)"].folder = "PD"
m["Previous PD (12)"].formatter = "DOUBLE[#,###.##%]"

m["PD (12) (Chg)"] = tt.where(
    ~m["Previous PD (12)"].isnull(), m["PD (12)"] - m["Previous PD (12)"]
)
m["PD (12) (Chg)"].folder = "PD"
m["PD (12) (Chg)"].formatter = "DOUBLE[+#,###.##%;-#,###.##%]"


# Lifetime PD and variations

# In[22]:


m["PD (LT)"] = tt.agg.mean(risk_engine_data["PDLT"])
m["PD (LT)"].folder = "PD"
m["PD (LT)"].formatter = "DOUBLE[#,###.##%]"

m["Opening PD (LT)"] = tt.agg.mean(loans_at_inception["Opening PDLT"])
m["Opening PD (LT)"].folder = "PD"

m["Previous PD (LT)"] = tt.shift(m["PD (LT)"], h["Reporting Date"], offset=1)
m["Previous PD (LT)"].folder = "PD"

m["PD (LT) (Chg)"] = tt.where(
    ~m["Previous PD (LT)"].isnull(), m["PD (LT)"] - m["Previous PD (LT)"]
)
m["PD (LT) (Chg)"].folder = "PD"

# Variation from opening
m["PD (LT) Variation"] = (m["PD (LT)"] - m["Opening PD (LT)"]) / m["Opening PD (LT)"]
m["PD (LT) Variation"].folder = "PD"
m["PD (LT) Variation"].formatter = "DOUBLE[+#,###.##%;-#,###.##%]"


# Visualizing LGD
# 
# 
# The Lost Given Default is the percentage that you can lose when the debtor defaults.

# In[23]:


m["LGD"] = tt.agg.mean(risk_engine_data["LGD"])
m["LGD"].folder = "LGD"
m["LGD"].formatter = "DOUBLE[#,###.##%]"

m["Previous LGD"] = tt.shift(m["LGD"], h["Reporting Date"], offset=1)
m["Previous LGD"].folder = "LGD"
m["Previous LGD"].formatter = "DOUBLE[#,###.##%]"


# 
# ECL computation

# In[24]:


ecl_stage_1 = tt.agg.sum_product(
    risk_engine_data["LGD"], risk_engine_data["EAD"], risk_engine_data["PD12"]
)

ecl_stage_2 = tt.agg.sum_product(
    risk_engine_data["LGD"], risk_engine_data["EAD"], risk_engine_data["PDLT"]
)

ecl_stage_3 = tt.agg.sum_product(risk_engine_data["LGD"], risk_engine_data["EAD"])


# In[25]:


##Now, the measure visible in the UI will pick the correct formula depending on the stage:

m["ECL"] = (
    tt.filter(ecl_stage_1, l["Stage"] == 1)
    + tt.filter(ecl_stage_2, l["Stage"] == 2)
    + tt.filter(ecl_stage_3, l["Stage"] == 3)
)
m["ECL"].folder = "ECL"


# In[26]:


cube.query(m["ECL"], levels=[l["Reporting Date"]])


# ## As usual, let's create measures to visualize the previous reporting date values and changes:

# In[27]:


m["Previous ECL"] = tt.shift(m["ECL"], h["Reporting Date"], offset=1)
m["Previous ECL"].folder = "ECL"

m["ECL (Chg)"] = tt.where(~m["Previous ECL"].isnull(), m["ECL"] - m["Previous ECL"])
m["ECL (Chg)"].folder = "ECL"

m["ECL (Chg %)"] = tt.where(m["Previous ECL"] != 0, m["ECL (Chg)"] / m["Previous ECL"])
m["ECL (Chg %)"].folder = "ECL"
m["ECL (Chg %)"].formatter = "DOUBLE[+#,###.##%;-#,###.##%]"

m["ECL of old contracts"] = tt.where(l["Reporting Date"] != l["issue_d"], m["ECL"])
m["ECL of old contracts"].folder = "ECL"

m["ECL (Chg without new contracts)"] = tt.where(
    ~m["Previous ECL"].isnull(), m["ECL of old contracts"] - m["Previous ECL"]
)
m["ECL (Chg without new contracts)"].folder = "ECL"

m["ECL (Chg % without new contracts)"] = tt.where(
    m["Previous ECL"] != 0, m["ECL (Chg without new contracts)"] / m["Previous ECL"]
)
m["ECL (Chg % without new contracts)"].folder = "ECL"
m["ECL (Chg % without new contracts)"].formatter = "DOUBLE[+#,###.##%;-#,###.##%]"


# ## ECL change explainers
# 
# ## ECL variation due to PD changes

# In[28]:


ecl_pd_explain_stage_1 = tt.agg.sum_product(
    risk_engine_data["LGD"],
    risk_engine_data["EAD"],
    risk_engine_data["Previous PD12"],
)
ecl_pd_explain_stage_2 = tt.agg.sum_product(
    risk_engine_data["LGD"],
    risk_engine_data["EAD"],
    risk_engine_data["Previous PDLT"],
)

m["ECL with previous PD"] = (
    tt.filter(ecl_pd_explain_stage_1, l["Stage"] == 1)
    + tt.filter(ecl_pd_explain_stage_2, l["Stage"] == 2)
    + tt.filter(ecl_stage_3, l["Stage"] == 3)
)

m["ECL variation due to PD changes"] = m["ECL"] - m["ECL with previous PD"]
m["ECL variation due to PD changes"].folder = "ECL"


# In[29]:


cube.query(
    m["ECL (Chg)"],
    m["ECL variation due to PD changes"],
    levels=[l["Reporting Date"]],
)


# ## ECL variation due to LGD changes

# In[30]:


ecl_lgd_explain_stage_1 = tt.agg.sum_product(
    risk_engine_data["Previous LGD"],
    risk_engine_data["EAD"],
    risk_engine_data["PD12"],
)
ecl_lgd_explain_stage_2 = tt.agg.sum_product(
    risk_engine_data["Previous LGD"],
    risk_engine_data["EAD"],
    risk_engine_data["PDLT"],
)
ecl_lgd_explain_stage_3 = tt.agg.sum_product(
    risk_engine_data["Previous LGD"], risk_engine_data["EAD"]
)

m["ECL with previous LGD"] = (
    tt.filter(ecl_lgd_explain_stage_1, l["Stage"] == 1)
    + tt.filter(ecl_lgd_explain_stage_2, l["Stage"] == 2)
    + tt.filter(ecl_lgd_explain_stage_3, l["Stage"] == 3)
)

m["ECL variation due to LGD changes"] = m["ECL"] - m["ECL with previous LGD"]
m["ECL variation due to LGD changes"].folder = "ECL"


# In[31]:


m["ecl_lgd_explain_stage_1"] = ecl_lgd_explain_stage_1


# In[32]:


cube.query(
    m["ECL (Chg)"],
    m["ECL variation due to PD changes"],
    m["ECL variation due to LGD changes"],
    levels=[l["Reporting Date"]],
)


# In[33]:


session.link()


#  ## ECL variation due to EAD changes

# In[34]:


ecl_ead_explain_stage_1 = tt.agg.sum_product(
    risk_engine_data["LGD"],
    risk_engine_data["Previous EAD"],
    risk_engine_data["PD12"],
)
ecl_ead_explain_stage_2 = tt.agg.sum_product(
    risk_engine_data["LGD"],
    risk_engine_data["Previous EAD"],
    risk_engine_data["PDLT"],
)
ecl_ead_explain_stage_3 = tt.agg.sum_product(
    risk_engine_data["LGD"], risk_engine_data["Previous EAD"]
)

m["ECL with previous EAD"] = (
    tt.filter(ecl_ead_explain_stage_1, l["Stage"] == 1)
    + tt.filter(ecl_ead_explain_stage_2, l["Stage"] == 2)
    + tt.filter(ecl_ead_explain_stage_3, l["Stage"] == 3)
)

m["ECL variation due to EAD changes"] = m["ECL"] - m["ECL with previous EAD"]
m["ECL variation due to EAD changes"].folder = "ECL"


# In[35]:


cube.query(
    m["ECL (Chg)"],
    m["ECL variation due to PD changes"],
    m["ECL variation due to LGD changes"],
    m["ECL variation due to EAD changes"],
    levels=[l["Reporting Date"]],
)


# ## Unexplained variation

# In[36]:


m["ECL unexplained variation"] = (
    m["ECL (Chg)"]
    - m["ECL variation due to PD changes"]
    - m["ECL variation due to LGD changes"]
    - m["ECL variation due to EAD changes"]
)


# In[37]:


cube.query(
    m["ECL (Chg)"],
    m["ECL variation due to PD changes"],
    m["ECL variation due to LGD changes"],
    m["ECL variation due to EAD changes"],
    m["ECL unexplained variation"],
    levels=[l["Reporting Date"]],
)


# ### Vintage analysis

# In[38]:


cube.query(m["contributors.COUNT"], levels=[l["Reporting Date"]]).head(5)


# In[39]:


PastDueDaysThresholds = cube.create_parameter_simulation(
    "PastDueDaysThresholds",
    measures={"PastDueDaysThreshold": 30.0},
    base_scenario_name=">30 days",
)


# In[40]:


PastDueDaysThresholds += (">60 days", 60.0)
PastDueDaysThresholds += (">90 days", 90.0)


# In[41]:


# Indicator - let's assume that a loan is classified as a past due due after 30 days of being "past due".
m["DaysPastDue"] = tt.agg.single_value(risk_engine_data["DaysPastDue"])

# Number of contracts past due:
m["Num_Contracts_Past_due"] = tt.agg.sum(
    tt.where(m["DaysPastDue"] > m["PastDueDaysThreshold"], 1.0, 0.0),
    scope=tt.OriginScope(l["id"]),
)

m["% past due"] = m["Num_Contracts_Past_due"] / m["contributors.COUNT"]
m["% past due"].formatter = "DOUBLE[#,###.##%]"


# In[42]:


cube.query(m["% past due"], levels=[l["Reporting Date"]])


# ### Annex I: LendingClub data viz

# In[43]:


cube2 = session.create_cube(lending_club_data, "Lending Club EDA")


# In[44]:


m2 = cube2.measures


# ## Loan amount

# In[45]:


# The listed amount of the loan applied for by the borrower.
m2["loan_amount"] = tt.agg.sum(lending_club_data["loan_amnt"])
m2["loan_amount"].folder = "LendingClub"
# Average loan amount:
m2["loan_amount.MEAN"] = tt.agg.mean(lending_club_data["loan_amnt"])
m2["loan_amount.MEAN"].folder = "LendingClub"


# In[46]:


session.visualize("Total loan amount")


# In[47]:


session.visualize("Average loan size by Credit Score")


# ## Interest rate

# In[48]:


session.visualize("Interest rates by credit score")


# ## Count loans

# In[49]:


session.visualize("Count loans")


# In[50]:


session.visualize("Proportion of loans purpose and by status")


# In[51]:


session.link()


# In[ ]:




