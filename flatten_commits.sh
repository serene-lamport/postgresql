branches=$(git branch | cut -c 3-)

for branch in $branches
do
    git checkout $branch
    git checkout --orphan $branch-orphan # Create a new orphan branch
    git add -A
    git commit -m "Flatten $branch"
    git branch -D $branch
    git checkout -b $branch
    git branch -D $branch-orphan
done