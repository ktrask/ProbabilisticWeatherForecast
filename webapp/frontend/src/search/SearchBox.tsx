// Place search: suggestions while typing (debounced, through /api/geocode),
// coordinates typed in directly, or the browser's own position.
import { type KeyboardEvent, useEffect, useId, useState } from "react";

import type { Place } from "../api/client";
import { MIN_QUERY_LENGTH, useGeocode } from "../api/queries";
import { useI18n } from "../i18n";

export interface Selection {
  lat: number;
  lon: number;
  name: string;
}

export const DEBOUNCE_MS = 250;

export function useDebounced<T>(value: T, ms: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setSettled(value), ms);
    return () => clearTimeout(timer);
  }, [value, ms]);
  return settled;
}

/** "52.26, 10.52", "52.26 10.52" or "52.26;10.52" -> the coordinates, if valid.
 *
 * A bare comma between two whole numbers is not taken as a separator: "52,26"
 * is how German writes 52.26, not the point 52° N 26° E. */
export function parseCoordinates(text: string): { lat: number; lon: number } | null {
  const match = text.trim().match(/^(-?\d{1,2}(?:\.\d+)?)(\s*[,;]\s*|\s+)(-?\d{1,3}(?:\.\d+)?)$/);
  if (!match) return null;
  const [, first = "", separator = "", second = ""] = match;
  if (separator === "," && !first.includes(".") && !second.includes(".")) return null;
  const lat = Number(first);
  const lon = Number(second);
  return Math.abs(lat) <= 90 && Math.abs(lon) <= 180 ? { lat, lon } : null;
}

export function placeLabel(place: Place): string {
  return [place.name, place.admin1, place.country].filter(Boolean).join(", ");
}

interface Option {
  key: string;
  label: string;
  detail?: string;
  selection: Selection;
}

export function SearchBox({ onSelect }: { onSelect: (selection: Selection) => void }) {
  const i18n = useI18n();
  const id = useId();
  const [text, setText] = useState("");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [locating, setLocating] = useState<"idle" | "busy" | "failed">("idle");
  const query = useDebounced(text, DEBOUNCE_MS);
  const coordinates = parseCoordinates(text);
  // Coordinates need no geocoder.
  const geocode = useGeocode(coordinates || parseCoordinates(query) ? "" : query, i18n.lang);

  const options: Option[] = [];
  if (coordinates) {
    const label = `${coordinates.lat}, ${coordinates.lon}`;
    options.push({ key: "coordinates", label, detail: i18n.t.coordinates, selection: { ...coordinates, name: label } });
  } else if (query.trim().length >= MIN_QUERY_LENGTH) {
    for (const place of geocode.data?.results ?? []) {
      options.push({
        key: `${place.lat},${place.lon}`,
        label: place.name,
        detail: [place.admin1, place.country].filter(Boolean).join(", "),
        selection: { lat: place.lat, lon: place.lon, name: [place.name, place.country].filter(Boolean).join(", ") },
      });
    }
  }
  const searching = !coordinates && text.trim().length >= MIN_QUERY_LENGTH && (geocode.isFetching || query !== text);
  const showList = open && text.trim().length >= MIN_QUERY_LENGTH;

  const choose = (option: Option | undefined) => {
    if (!option) return;
    onSelect(option.selection);
    setText("");
    setOpen(false);
    setActive(0);
  };

  const onKey = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setOpen(true);
      setActive((a) => Math.min(options.length - 1, a + 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActive((a) => Math.max(0, a - 1));
    } else if (event.key === "Enter") {
      event.preventDefault();
      choose(options[active] ?? options[0]);
    } else if (event.key === "Escape") {
      setOpen(false);
    }
  };

  const locate = () => {
    if (!("geolocation" in navigator)) {
      setLocating("failed");
      return;
    }
    setLocating("busy");
    navigator.geolocation.getCurrentPosition(
      (position) => {
        setLocating("idle");
        onSelect({
          lat: Number(position.coords.latitude.toFixed(4)),
          lon: Number(position.coords.longitude.toFixed(4)),
          name: i18n.t.myLocation,
        });
      },
      () => setLocating("failed"),
      { timeout: 10_000, maximumAge: 10 * 60_000 },
    );
  };

  const listId = `${id}-list`;
  return (
    <div className="search">
      <label htmlFor={`${id}-input`} className="visually-hidden">
        {i18n.t.searchLabel}
      </label>
      <div className="search-row">
        <input
          id={`${id}-input`}
          type="search"
          role="combobox"
          autoComplete="off"
          spellCheck={false}
          aria-expanded={showList}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={showList && options[active] ? `${id}-${active}` : undefined}
          placeholder={i18n.t.searchPlaceholder}
          value={text}
          onChange={(event) => {
            setText(event.target.value);
            setOpen(true);
            setActive(0);
          }}
          onKeyDown={onKey}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
        />
        <button type="button" className="locate" onClick={locate} disabled={locating === "busy"}>
          {locating === "busy" ? i18n.t.locating : i18n.t.myLocation}
        </button>
      </div>
      {locating === "failed" && <p className="search-note">{i18n.t.locationDenied}</p>}
      {showList && (
        <ul id={listId} role="listbox" className="suggestions">
          {options.map((option, index) => (
            <li
              key={option.key}
              id={`${id}-${index}`}
              role="option"
              aria-selected={index === active}
              onMouseDown={(event) => {
                event.preventDefault();
                choose(option);
              }}
              onMouseEnter={() => setActive(index)}
            >
              <span className="option-label">{option.label}</span>
              {option.detail && <span className="option-detail">{option.detail}</span>}
            </li>
          ))}
          {!options.length && (
            <li className="empty" aria-disabled="true">
              {searching ? i18n.t.searching : i18n.t.noResults}
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
