
CREATE TABLE items (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT,
    description TEXT,
    thumbnail TEXT NOT NULL,
    category TEXT,
    tags TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_items_slug ON items (slug) WHERE slug IS NOT NULL;
CREATE INDEX idx_items_category ON items (category);
CREATE INDEX idx_items_tags ON items USING GIN (tags);
