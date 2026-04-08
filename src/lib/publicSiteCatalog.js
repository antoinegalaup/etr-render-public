const FRONT_MEDIA_PATH = "/wp-content/uploads/vikbooking/front";

function joinBaseUrl(baseUrl, pathname) {
  const normalizedBase = `${baseUrl || ""}`.trim().replace(/\/$/, "");
  return normalizedBase ? `${normalizedBase}${pathname}` : pathname;
}

function titleCase(value) {
  return `${value || ""}`
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function fileCaption(fileName) {
  return titleCase(
    `${fileName || ""}`
      .replace(/\.[a-z0-9]+$/i, "")
      .replace(/^(big_|thumb_|mini_)+/i, "")
      .replace(/^[0-9]+_?/i, "")
      .replace(/_/g, " ")
      .replace(/-/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function mediaItem(propertyName, category, fileName, wordpressBaseUrl, index) {
  const path = `${FRONT_MEDIA_PATH}/${fileName}`;
  return {
    id: `${category}_${index + 1}`,
    category,
    path,
    src: joinBaseUrl(wordpressBaseUrl, path),
    alt: `${propertyName} ${fileCaption(fileName).toLowerCase()}`,
    caption: fileCaption(fileName)
  };
}

function mediaGroup(propertyName, category, fileNames, wordpressBaseUrl) {
  return fileNames.map((fileName, index) =>
    mediaItem(propertyName, category, fileName, wordpressBaseUrl, index)
  );
}

export function buildPublicProperty(definition, wordpressBaseUrl = "") {
  const media = {
    hero: mediaGroup(definition.name, "hero", definition.media_manifest.hero, wordpressBaseUrl),
    editorial: mediaGroup(
      definition.name,
      "editorial",
      definition.media_manifest.editorial,
      wordpressBaseUrl
    ),
    gallery: mediaGroup(
      definition.name,
      "gallery",
      definition.media_manifest.gallery,
      wordpressBaseUrl
    ),
    detail: mediaGroup(
      definition.name,
      "detail",
      definition.media_manifest.detail,
      wordpressBaseUrl
    ),
    drone: mediaGroup(definition.name, "drone", definition.media_manifest.drone, wordpressBaseUrl),
    fallback: mediaGroup(
      definition.name,
      "fallback",
      definition.media_manifest.fallback,
      wordpressBaseUrl
    )
  };

  return {
    room_id: definition.room_id,
    slug: definition.slug,
    name: definition.name,
    tagline: definition.tagline,
    location_label: definition.location_label,
    short_description: definition.short_description,
    long_description: definition.long_description,
    capacity: definition.capacity,
    base_price_hint: definition.base_price_hint,
    badges: definition.badges,
    amenities: definition.amenities,
    highlights: definition.highlights,
    sleeping_layout: definition.sleeping_layout,
    booking_rules: definition.booking_rules,
    price_summary: definition.price_summary,
    testimonial: definition.testimonial || null,
    story_sections: definition.story_sections,
    media,
    hero_image: media.hero[0]?.src || media.fallback[0]?.src || null,
    hero_media: media.hero[0] || media.fallback[0] || null
  };
}

export const PUBLIC_PROPERTY_DEFINITIONS = [
  {
    room_id: 6,
    slug: "villa-esencia",
    name: "Villa Esencia",
    tagline: "Private beachfront villa for long-table family stays",
    location_label: "Little Exuma, The Bahamas",
    short_description:
      "A full-home coastal hideaway with direct beach access, a pool deck, and layered indoor-outdoor living.",
    long_description:
      "Villa Esencia is the hero stay in the collection: sunrise swimming, shaded dining, and enough room for multi-generation groups to spread out without losing the sense of being together.",
    capacity: 8,
    base_price_hint: 1000,
    badges: ["Beachfront", "Sleeps 8", "Pool deck", "Office"],
    amenities: [
      "Beachfront access",
      "Pool deck and lounge chairs",
      "Four bedrooms",
      "Indoor-outdoor dining",
      "Large family kitchen",
      "Fast Wi-Fi",
      "Housekeeping coordination",
      "Private arrival planning"
    ],
    highlights: [
      { label: "Best for", value: "Multi-family and celebration stays" },
      { label: "Atmosphere", value: "Warm editorial luxury with Exuma light" },
      { label: "Signature moment", value: "Sunset dinner above the beach stairs" }
    ],
    sleeping_layout: [
      { label: "Primary suite", detail: "Ocean-facing king suite with bathroom" },
      { label: "Guest rooms", detail: "Three additional bedrooms for kids and couples" },
      { label: "Shared living", detail: "Open kitchen, dining, and lounge facing the sea" }
    ],
    booking_rules: [
      { label: "Minimum stay", detail: "1 night" },
      { label: "Check-in", detail: "After 4:00 PM" },
      { label: "Check-out", detail: "Before 10:00 AM" },
      { label: "Good to know", detail: "Ideal for longer family stays and milestone trips" }
    ],
    price_summary: {
      starting_from: 1000,
      seasonal_note: "Rates flex seasonally and on holidays.",
      stay_note: "Longer stays unlock the strongest value."
    },
    testimonial: null,
    story_sections: [
      {
        title: "Space That Breathes",
        body:
          "The public rooms are wide and bright, so the house works equally well for quiet mornings, remote-work pockets, and big communal dinners."
      },
      {
        title: "Close to the Water",
        body:
          "The beach stairs, pool deck, and porch sequencing create a layered transition from shade to sea instead of a single flat terrace."
      }
    ],
    media_manifest: {
      hero: ["05_beach_porch_final_400k.jpg", "04_pool_deck_beach_and_umbrellas.jpg"],
      editorial: [
        "03_stairs_from_the_house_to_the_beach.jpg",
        "18_sunset_on_the_deck_329k.jpg",
        "08_living_room_mirror_reflecting_the_beach_400k.jpg"
      ],
      gallery: [
        "04_pool_deck_beach_and_umbrellas.jpg",
        "01_villa_front_at_sunset_400k.jpg",
        "07_living_room_-_kitchen_-_dining_room_400k.jpg",
        "09_dining_table_600k.jpeg",
        "10_dining_and_kitchen_post_gpt_400k.jpg",
        "11_bedroom_1_beach_and_tv_reflection_chatgpt_final_400k.jpg",
        "12_bedroom_2_post_chatgpt_processing_400k.jpg",
        "13_bedroom_3_post_gpt_400k.jpg",
        "16_bedroom_4_post_gpt_400k.jpg"
      ],
      detail: [
        "03_stairs_from_the_house_to_the_beach.jpg",
        "15_bathroom_detail_400k.jpeg",
        "17_bathroom_detail_500k.jpeg"
      ],
      drone: ["dji_0336.jpg", "house_front_seen_from_the_beach_stairs_medium.jpeg"],
      fallback: ["beach_from_the_sun_deck_medium.jpeg", "villa_front_at_sunset_medium.jpeg"]
    }
  },
  {
    room_id: 5,
    slug: "lake-cottage",
    name: "Lake Cottage",
    tagline: "One-bedroom cottage with a hot tub and a short walk to Tropic of Cancer beach",
    location_label: "Little Exuma, The Bahamas",
    short_description:
      "Cozy one-bedroom cottage with a hot tub, lake outlook, and a short walk to Tropic of Cancer beach.",
    long_description:
      "Lake Cottage is designed for a quieter stay: a king bedroom, a large porch with a two-seat hot tub, and a calm lake-facing setting that still keeps the beach within a short walk.",
    capacity: 2,
    base_price_hint: 240,
    badges: ["One-bedroom", "Sleeps 2", "Hot tub", "2-minute beach walk"],
    amenities: [
      "King bedroom",
      "Two-seat hot tub",
      "Large covered porch",
      "Washer and dryer",
      "Dishwasher and induction cooktop",
      "65-inch smart TV and sound system",
      "Quiet lake outlook",
      "Short walk to the beach",
      "Fast Wi-Fi",
      "Easy arrival logistics"
    ],
    highlights: [
      { label: "Best for", value: "Couples looking for a quieter stay" },
      { label: "Atmosphere", value: "Private, calm, and lightly polished" },
      { label: "Signature moment", value: "Early coffee on the porch before the beach walk" }
    ],
    sleeping_layout: [
      { label: "Bedroom", detail: "King-size bed with dedicated A/C and ceiling fan" },
      { label: "Main room", detail: "Open kitchen and living space with island seating and lounge area" },
      { label: "Outdoor space", detail: "Large porch with hot tub, lounge chairs, and seating area" }
    ],
    booking_rules: [
      { label: "Minimum stay", detail: "1 night" },
      { label: "Check-in", detail: "After 4:00 PM" },
      { label: "Check-out", detail: "Before 10:00 AM" },
      { label: "Good to know", detail: "Best suited to one couple rather than a larger group" }
    ],
    price_summary: {
      starting_from: 240,
      seasonal_note: "Direct rates follow the active VikBooking pricing table.",
      stay_note: "A quiet one-bedroom base with beach access close by."
    },
    testimonial: null,
    story_sections: [
      {
        title: "Quiet Lake Outlook",
        body:
          "The cottage sits back from the main beach frontage, which makes it feel more private and calm while still keeping the shoreline within a short walk."
      },
      {
        title: "Compact, Finished, Easy",
        body:
          "The footprint is efficient rather than oversized: one strong bedroom, a polished kitchen-living room, and a porch that does most of the atmosphere work."
      }
    ],
    media_manifest: {
      hero: ["lc_entrance.jpg", "image0.jpeg"],
      editorial: ["image6.jpeg", "1354image1.jpeg", "image5.jpeg"],
      gallery: [
        "lc_entrance.jpg",
        "image0.jpeg",
        "image6.jpeg",
        "image5.jpeg",
        "1354image1.jpeg",
        "image3.jpeg",
        "image4.jpeg",
        "img_7588.jpeg"
      ],
      detail: ["image3.jpeg", "image4.jpeg", "img_7588.jpeg"],
      drone: ["house_front_seen_from_the_beach_stairs_medium.jpeg"],
      fallback: ["lc_entrance.jpg", "image0.jpeg"]
    }
  },
  {
    room_id: 1,
    slug: "kl-cottage",
    name: "KeyLime Cottage",
    tagline: "Three-bedroom beachfront house with a private gazebo on Tropic of Cancer beach",
    location_label: "Little Exuma, The Bahamas",
    short_description:
      "Beautiful three-bedroom cottage located right on the water with a private gazebo and direct beach access.",
    long_description:
      "KeyLime Cottage is the more relaxed beachfront house in the collection: three bedrooms, a wraparound porch, direct access to Tropic of Cancer beach, and a private gazebo for most of the day’s living.",
    capacity: 6,
    base_price_hint: 550,
    badges: ["Beachfront", "Sleeps 6", "3 bedrooms", "Private gazebo"],
    amenities: [
      "Private beachfront gazebo",
      "Wraparound porch",
      "Three ensuite bathrooms",
      "Open kitchen, dining, and living room",
      "Laundry and half bathroom",
      "Kayaks and paddleboards",
      "Snorkeling gear",
      "Two outdoor showers",
      "Fast Wi-Fi",
      "Quiet beachfront setting"
    ],
    highlights: [
      { label: "Best for", value: "Families or friends sharing a full beachfront house" },
      { label: "Atmosphere", value: "Warm, bright, and directly tied to the beach" },
      { label: "Signature moment", value: "Sunset from the gazebo over the water" }
    ],
    sleeping_layout: [
      { label: "Bedroom 1", detail: "King bed, ensuite bath, and beach porch access" },
      { label: "Bedroom 2", detail: "King bed, ensuite bath, and garden porch access" },
      { label: "Bedroom 3", detail: "Two full beds with its own ensuite bath" }
    ],
    booking_rules: [
      { label: "Minimum stay", detail: "1 night" },
      { label: "Check-in", detail: "After 4:00 PM" },
      { label: "Check-out", detail: "Before 10:00 AM" },
      { label: "Good to know", detail: "Built for outdoor-heavy days with the house supporting the beach rhythm" }
    ],
    price_summary: {
      starting_from: 550,
      seasonal_note: "Direct rates follow the active VikBooking pricing table.",
      stay_note: "A true beachfront house with enough room for six guests."
    },
    testimonial: null,
    story_sections: [
      {
        title: "Beachfront House, Not Just a Room",
        body:
          "KeyLime Cottage is built around the beach first: gazebo, porch, and shoreline access all come before the interior footprint."
      },
      {
        title: "Three Bedrooms, Easy Rhythm",
        body:
          "The layout is practical for shared stays, with two king rooms for couples and a third bedroom with two full beds for younger guests or overflow."
      }
    ],
    media_manifest: {
      hero: ["image8.jpeg", "a083fd91-0e3b-4442-92ea-7ae3fa8103c5.jpg"],
      editorial: ["41b63e49-1ca2-449b-b1cd-a12efdac5ff7.jpg", "stairs_to_the_beach.jpg", "patio.jpg"],
      gallery: [
        "image8.jpeg",
        "a083fd91-0e3b-4442-92ea-7ae3fa8103c5.jpg",
        "41b63e49-1ca2-449b-b1cd-a12efdac5ff7.jpg",
        "stairs_to_the_beach.jpg",
        "20220331_110721716_ios.jpg",
        "patio.jpg",
        "patio_dining.jpg",
        "kitchen.jpeg",
        "master_bedroom_4.jpg",
        "image1.jpeg",
        "bedroom_2.jpeg",
        "bedroom_3.jpeg",
        "img_7867.jpeg"
      ],
      detail: ["kitchen.jpeg", "master_bedroom_4.jpg", "img_7867.jpeg"],
      drone: ["dji_0336.jpg"],
      fallback: ["image8.jpeg", "a083fd91-0e3b-4442-92ea-7ae3fa8103c5.jpg"]
    }
  }
];

export function getPublicPropertyCards({ wordpressBaseUrl = "" } = {}) {
  return PUBLIC_PROPERTY_DEFINITIONS.map((definition) => {
    const property = buildPublicProperty(definition, wordpressBaseUrl);
    return {
      room_id: property.room_id,
      slug: property.slug,
      name: property.name,
      tagline: property.tagline,
      location_label: property.location_label,
      short_description: property.short_description,
      capacity: property.capacity,
      hero_image: property.hero_image,
      base_price_hint: property.base_price_hint,
      badges: property.badges
    };
  });
}

export function getPublicPropertyDetail(identifier, { wordpressBaseUrl = "" } = {}) {
  const normalized = `${identifier || ""}`.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  const definition = PUBLIC_PROPERTY_DEFINITIONS.find(
    (candidate) =>
      `${candidate.room_id}` === normalized ||
      candidate.slug.toLowerCase() === normalized ||
      candidate.name.toLowerCase() === normalized
  );
  if (!definition) {
    return null;
  }
  return buildPublicProperty(definition, wordpressBaseUrl);
}
