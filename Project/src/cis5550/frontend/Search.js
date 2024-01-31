// at most two apis to call for back end, see details in the fetch function below

function search() {
    const query = document.getElementById('searchInput').value.toLowerCase();

    //the api to call for back end
    fetch(`/query?query=${query}`)
    .then(response => response.text())
    .then(response => {
      const data = JSON.parse(response);
      return data;
    })
    .then(urls => {
      const info = urls.map(url => {
        const encodeurl = encodeURIComponent(url.url);
        return fetch(`/query/${encodeurl}`)
        .then(response => response.json())
        .then(data => ({
          title: data.title,
          url: data.url,
          abstract: data.abstract
        }));
      });
      return Promise.all(info);
    })
    .then(results => {
      showResults(results, 1, 10);
    })
    .catch(error => console.error('Fetch error: ', error));

  return false;
}

function showResults(results, page, pageSize) {
  const startIndex = (page - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const paginatedResults = results.slice(startIndex, endIndex);

  const resultsDiv = document.getElementById('results');
  resultsDiv.innerHTML = '';

  if (paginatedResults.length === 0) {
    resultsDiv.textContent = 'Sorry..Cannot find match';
  } else {
    const ul = document.createElement('ul');
    paginatedResults.forEach((result) => {
      const li = document.createElement('li');

      //title
      const title = document.createElement('h4');
      const a = document.createElement('a');
      a.href = result.url;
      a.textContent = result.title;
      title.appendChild(a);
      li.appendChild(title);

      //abtract
      const abstract = document.createElement('p');
      abstract.textContent = result.abstract;
      li.appendChild(abstract);

      ul.appendChild(li);
    });
    resultsDiv.appendChild(ul);

    if (results.length > pageSize) {
      const numPages = Math.ceil(results.length / pageSize);
      const paginationContainer = document.createElement('div');
      paginationContainer.classList.add('pagination-container');
      const paginationDiv = document.createElement('div');
      paginationDiv.classList.add('pagination');
      
      const frontPageButton = document.createElement('button');
      frontPageButton.textContent = 'Prev';
      frontPageButton.classList.add('prev-button');
      frontPageButton.addEventListener('click', () => {
        if (page > 1) {
          showResults(results, page - 1, pageSize);
        }
      });
      paginationContainer.appendChild(frontPageButton);

      for (let i = 1; i <= numPages; i++) {
        const pageButton = document.createElement('button');
        pageButton.textContent = i;
        pageButton.addEventListener('click', () => {
          showResults(results, i, pageSize);
        });
        if (i === page) {
          pageButton.classList.add('current');
        }
        paginationDiv.appendChild(pageButton);
      }
      paginationContainer.appendChild(paginationDiv);

      const nextPageButton = document.createElement('button');
      nextPageButton.textContent = 'Next';
      nextPageButton.classList.add('next-button');
      nextPageButton.addEventListener('click', () => {
        if (page < numPages) {
          showResults(results, page + 1, pageSize);
        }
      });
      paginationContainer.appendChild(nextPageButton);

      resultsDiv.appendChild(paginationContainer);
    }
  }
}