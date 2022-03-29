"""
This is a metadata scraping plugin for DocumentCloud.
It receives data from DocumentCloud via the request
 dispatch and writes data back to
DocumentCloud using the standard API
"""

from documentcloud.addon import AddOn
import csv


class MetaDataScrape(AddOn):
    """A metadata scraping Add-On for DocumentCloud."""

    def main(self):
        # fetch your add-on specific data
        self.set_message("Beginning metadata scraping!")

        # preset header + metadata list
        header = [
            "id",
            "title",
            "privacy level",
            "asset-url",
            "contributor",
            "created at date",
            "description",
            "full text url",
            "pdf url",
            "page count",
            "Tags",
            "Key Value Pairs",
        ]
        metadata_list = []  # list o lists containing metadata for each document

        # takes the document object and an empty array as input, and places the document metadata into the array
        def set_data(doc, doc_metadata):

            # document description break fix
            description = getattr(doc, "description", "")

            doc_metadata = [
                doc.id,
                doc.title,
                doc.access,
                doc.asset_url,
                doc.contributor,
                doc.created_at,
                description,
                doc.full_text_url,
                doc.pdf_url,
                doc.page_count,
            ]

            # separate key values and tags into two separate arrays
            key_values = doc.data
            tags = ""

            # are there any tags?
            tags = key_values.pop("_tag", "")

            doc_metadata.append(tags)
            doc_metadata.append(key_values)

            return doc_metadata

        # retrieve metadata information from each document.
        if self.documents:
            for doc_id in self.documents:
                document = self.client.documents.get(doc_id)
                # set the metadata
                metadata_list.append(set_data(document, []))
        elif self.query:
            documents = self.client.documents.search(self.query)
            for document in documents:
                metadata_list.append(set_data(document, []))

        # the id of the first document + how many more documents will be the name of the file
        try:
            first_title = metadata_list[0][1]
        except IndexError:
            first_title = ""

        with open(
            f"metadata_for_{str(first_title)}_+{str(len(metadata_list)-1)}.csv", "w+"
        ) as file_:
            writer = csv.writer(file_)

            # FORMAT HEADER
            writer.writerow(header)

            for row in metadata_list:
                # FORMAT THE DATA
                writer.writerow(row)

            self.upload_file(file_)
        self.set_message("Metadata scraping end!")


if __name__ == "__main__":
    MetaDataScrape().main()
