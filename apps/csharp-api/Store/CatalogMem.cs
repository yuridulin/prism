using Prism.Api.Models;

namespace Prism.Api.Store;

public sealed class CatalogMem
{
    private readonly object _gate = new();
    private readonly Dictionary<uint, Tag> _data = [];

    public void Upsert(IReadOnlyList<Tag> tags)
    {
        lock (_gate)
        {
            foreach (var tag in tags)
            {
                _data[tag.Id] = tag;
            }
        }
    }

    public IReadOnlyList<Tag> List()
    {
        lock (_gate)
        {
            return _data.Values.ToList();
        }
    }
}
