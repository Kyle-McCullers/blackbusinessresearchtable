# SAM.gov public extract: manual download (for the `OY` Black American Owned count)

Goal: get one recent monthly SAM.gov public entity file so BBRT can count firms with business-type code `OY`
before deciding how to ingest them. No code is needed for this step.

1. Go to https://sam.gov and choose **Sign In**. If you have no account, create one (SAM.gov uses Login.gov).
   Use a personal account (not an entity registration).
2. Open **Data Services**, then **Entity Registration** (direct link:
   https://sam.gov/data-services/Entity%20Registration). Under **My Data Access** choose **Entity Management**.
3. Find the **Public** monthly files. Names look like `SAM_PUBLIC_UTF-8_MONTHLY_V2_YYYYMMDD.ZIP`
   (UTF-8) and `SAM_PUBLIC_MONTHLY_V2_YYYYMMDD.ZIP` (ASCII). Download the **most recent UTF-8** file.
4. Save it, unzipped or not, in `~/BBRT-private/sam/` (create the folder if needed). Do not put it in the repo.
5. Tell Claude the file name. Claude counts `OY`, `23` (minority-owned) and `27` records and reports before anything is built.

Optional, for automation later: on https://sam.gov/workspace/profile/account-details use **Public API Key**
(click the eye icon, enter the one-time password emailed to you). SAM API keys are, I believe, valid for a limited
period (about 90 days), which matters for a quarterly job; check before relying on one.

Layout reference: https://open.gsa.gov/api/sam-entity-extracts-api/ and the Public V2 extract layout PDF linked there.
Business type codes of interest: `OY` Black American Owned (self-selected), `23` Minority-Owned Business,
`27` Self-Certified Small Disadvantaged Business, `HQ` DOT-certified DBE; SBA 8(a) appears in the SBA business-types columns.
