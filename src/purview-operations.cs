using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Analytics.Purview.Catalog;
using Azure.Identity;

namespace DatabaseDDL{ 

    public class PurviewOperations{
        public static async Task<JsonDocument> GetPurviewJsonResponse(ClientSecretCredential cred, string strPurviewUri, string strType, string strQualifiedName)
        {
            var purviewClient = new PurviewCatalogClient(new Uri(strPurviewUri), cred);
            var rp = await purviewClient.Entities.GetByUniqueAttributesAsync(strType,null, null,strQualifiedName);
            var responseDocument = JsonDocument.Parse(rp.Content);

            return (responseDocument);
        }

    }
}